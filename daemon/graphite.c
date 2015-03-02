/*
 * graphite.c
 *
 *  Created on: Jan 19, 2015
 *      Author: fmetz
 */
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

#include "log.h"
#include "call.h"
#include "socket.h"

static socket_t graphite_sock;
static const endpoint_t *graphite_ep;
static struct callmaster* cm=0;
//struct totalstats totalstats_prev;
static time_t next_run;

int connect_to_graphite_server(const endpoint_t *ep) {
	graphite_ep = ep;

	ilog(LOG_INFO, "Connecting to graphite server %s", endpoint_print_buf(ep));

	if (connect_socket(&graphite_sock, SOCK_STREAM, ep)) {
		ilog(LOG_ERROR,"Couldn't make socket for connecting to graphite.");
		return -1;
	}

	ilog(LOG_INFO, "Graphite server connected.");

	return 0;
}

int send_graphite_data() {

	int rc=0;

	if (graphite_sock.fd < 0) {
		ilog(LOG_ERROR,"Graphite socket is not connected.");
		return -1;
	}

	// format hostname "." totals.subkey SPACE value SPACE timestamp
	char hostname[256];
	rc = gethostname(hostname,256);
	if (rc<0) {
		ilog(LOG_ERROR, "Could not retrieve host name information.");
		goto error;
	}

	char data_to_send[8192];
	char* ptr = data_to_send;

	struct totalstats ts;

	/* atomically copy values to stack and reset to zero */
	atomic64_local_copy_zero_struct(&ts, &cm->totalstats_interval, total_timeout_sess);
	atomic64_local_copy_zero_struct(&ts, &cm->totalstats_interval, total_silent_timeout_sess);
	atomic64_local_copy_zero_struct(&ts, &cm->totalstats_interval, total_regular_term_sess);
	atomic64_local_copy_zero_struct(&ts, &cm->totalstats_interval, total_forced_term_sess);
	atomic64_local_copy_zero_struct(&ts, &cm->totalstats_interval, total_relayed_packets);
	atomic64_local_copy_zero_struct(&ts, &cm->totalstats_interval, total_relayed_errors);
	atomic64_local_copy_zero_struct(&ts, &cm->totalstats_interval, total_nopacket_relayed_sess);
	atomic64_local_copy_zero_struct(&ts, &cm->totalstats_interval, total_oneway_stream_sess);

	mutex_lock(&cm->totalstats_interval.total_average_lock);
	ts.total_average_call_dur = cm->totalstats_interval.total_average_call_dur;
	ts.total_managed_sess = cm->totalstats_interval.total_managed_sess;
	ZERO(ts.total_average_call_dur);
	ZERO(ts.total_managed_sess);
	mutex_unlock(&cm->totalstats_interval.total_average_lock);

	rc = sprintf(ptr,"%s.totals.average_call_dur.tv_sec %llu %llu\n",hostname, (unsigned long long) ts.total_average_call_dur.tv_sec,(unsigned long long)g_now.tv_sec); ptr += rc;
	rc = sprintf(ptr,"%s.totals.average_call_dur.tv_usec %lu %llu\n",hostname, ts.total_average_call_dur.tv_usec,(unsigned long long)g_now.tv_sec); ptr += rc;
	rc = sprintf(ptr,"%s.totals.forced_term_sess "UINT64F" %llu\n",hostname, atomic64_get_na(&ts.total_forced_term_sess),(unsigned long long)g_now.tv_sec); ptr += rc;
	rc = sprintf(ptr,"%s.totals.managed_sess "UINT64F" %llu\n",hostname, ts.total_managed_sess,(unsigned long long)g_now.tv_sec); ptr += rc;
	rc = sprintf(ptr,"%s.totals.nopacket_relayed_sess "UINT64F" %llu\n",hostname, atomic64_get_na(&ts.total_nopacket_relayed_sess),(unsigned long long)g_now.tv_sec); ptr += rc;
	rc = sprintf(ptr,"%s.totals.oneway_stream_sess "UINT64F" %llu\n",hostname, atomic64_get_na(&ts.total_oneway_stream_sess),(unsigned long long)g_now.tv_sec); ptr += rc;
	rc = sprintf(ptr,"%s.totals.regular_term_sess "UINT64F" %llu\n",hostname, atomic64_get_na(&ts.total_regular_term_sess),(unsigned long long)g_now.tv_sec); ptr += rc;
	rc = sprintf(ptr,"%s.totals.relayed_errors "UINT64F" %llu\n",hostname, atomic64_get_na(&ts.total_relayed_errors),(unsigned long long)g_now.tv_sec); ptr += rc;
	rc = sprintf(ptr,"%s.totals.relayed_packets "UINT64F" %llu\n",hostname, atomic64_get_na(&ts.total_relayed_packets),(unsigned long long)g_now.tv_sec); ptr += rc;
	rc = sprintf(ptr,"%s.totals.silent_timeout_sess "UINT64F" %llu\n",hostname, atomic64_get_na(&ts.total_silent_timeout_sess),(unsigned long long)g_now.tv_sec); ptr += rc;
	rc = sprintf(ptr,"%s.totals.timeout_sess "UINT64F" %llu\n",hostname, atomic64_get_na(&ts.total_timeout_sess),(unsigned long long)g_now.tv_sec); ptr += rc;

	rc = write(graphite_sock.fd, data_to_send, ptr - data_to_send);
	if (rc<0) {
		ilog(LOG_ERROR,"Could not write to graphite socket. Disconnecting graphite server.");
		goto error;
	}
	return 0;

error:
	close_socket(&graphite_sock);
	return -1;
}

void graphite_loop_run(struct callmaster* callmaster, int seconds) {

	int rc=0;

	gettimeofday(&g_now, NULL);
	if (g_now.tv_sec < next_run)
		goto sleep;

	next_run = g_now.tv_sec + seconds;

	if (!cm)
		cm = callmaster;

	if (graphite_sock.fd < 0) {
		rc = connect_to_graphite_server(graphite_ep);
	}

	if (rc>=0) {
		rc = send_graphite_data();
		if (rc<0) {
			ilog(LOG_ERROR,"Sending graphite data failed.");
		}
	}

sleep:
	usleep(100000);
}

void graphite_loop(void *d) {
	struct callmaster *cm = d;

	if (!cm->conf.graphite_interval) {
		ilog(LOG_WARNING,"Graphite send interval was not set. Setting it to 1 second.");
		cm->conf.graphite_interval=1;
	}

	connect_to_graphite_server(&cm->conf.graphite_ep);

	while (!g_shutdown)
		graphite_loop_run(cm,cm->conf.graphite_interval); // time in seconds
}
