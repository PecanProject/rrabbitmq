#include <R.h>
#include <Rinternals.h>
#include <stdlib.h>
#include <R_ext/Rdynload.h>

extern SEXP rabbitmq_connect(SEXP host);
extern SEXP rabbitmq_close();
extern SEXP rabbitmq_create_queue(SEXP queue);
extern SEXP rabbitmq_destroy_queue(SEXP queue);
extern SEXP rabbitmq_publish(SEXP queue, SEXP msg);
extern SEXP rabbitmq_read_message(SEXP queue, SEXP timeout);
extern SEXP rabbitmq_ack_message(SEXP message);
extern SEXP rabbitmq_nack_message(SEXP message);

static const R_CallMethodDef CallEntries[] = {
  {"c_rabbitmq_connect",       (DL_FUNC) &rabbitmq_connect,       1},
  {"c_rabbitmq_close",         (DL_FUNC) &rabbitmq_close,         0},
  {"c_rabbitmq_create_queue",  (DL_FUNC) &rabbitmq_create_queue,  1},
  {"c_rabbitmq_destroy_queue", (DL_FUNC) &rabbitmq_destroy_queue, 1},
  {"c_rabbitmq_publish",       (DL_FUNC) &rabbitmq_publish,       2},
  {"c_rabbitmq_read_message",  (DL_FUNC) &rabbitmq_read_message,  2},
  {"c_rabbitmq_ack_message",   (DL_FUNC) &rabbitmq_ack_message,   1},
  {"c_rabbitmq_nack_message",  (DL_FUNC) &rabbitmq_nack_message,  1},
  {NULL, NULL, 0}
};

void R_init_rrabbitmq(DllInfo *dll) {
  R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
  R_useDynamicSymbols(dll, FALSE);
}
