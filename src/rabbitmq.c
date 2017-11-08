#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <amqp_ssl_socket.h>
#include <R.h>
#include <Rinternals.h>

amqp_socket_t *socket = NULL;
amqp_connection_state_t conn;

// ----------------------------------------------------------------------
// Helper function to check status of RabbitMQ call
// ----------------------------------------------------------------------
int amqp_check_status(amqp_rpc_reply_t x, char const *context) {
  //Rprintf("%d %s\n", x.reply_type, context);
  switch (x.reply_type) {
  case AMQP_RESPONSE_NORMAL:
    return TRUE;

  case AMQP_RESPONSE_NONE:
    REprintf("%s: missing RPC reply type!\n", context);
    break;

  case AMQP_RESPONSE_LIBRARY_EXCEPTION:
    REprintf("%s: %s\n", context, amqp_error_string2(x.library_error));
    break;

  case AMQP_RESPONSE_SERVER_EXCEPTION:
    switch (x.reply.id) {
    case AMQP_CONNECTION_CLOSE_METHOD: {
      amqp_connection_close_t *m = (amqp_connection_close_t *) x.reply.decoded;
      REprintf("%s: server connection error %uh, message: %.*s\n",
              context,
              m->reply_code,
              (int) m->reply_text.len, (char *) m->reply_text.bytes);
      break;
    }
    case AMQP_CHANNEL_CLOSE_METHOD: {
      amqp_channel_close_t *m = (amqp_channel_close_t *) x.reply.decoded;
      REprintf("%s: server channel error %uh, message: %.*s\n",
              context,
              m->reply_code,
              (int) m->reply_text.len, (char *) m->reply_text.bytes);
      break;
    }
    default:
      REprintf("%s: unknown server error, method id 0x%08X\n", context, x.reply.id);
      break;
    }
    break;
  }

  return FALSE;
}

// ----------------------------------------------------------------------
// Helper function to check return code of RabbitMQ call
// ----------------------------------------------------------------------
int amqp_check_error(int x, char const *context) {
  if (x < 0) {
    REprintf("%s: %s\n", context, amqp_error_string2(x));
    return FALSE;
  }
  return TRUE;
}

// ----------------------------------------------------------------------
// Open connection to RabbitMQ
// ----------------------------------------------------------------------
SEXP rabbitmq_connect(SEXP uri) {
  struct amqp_connection_info ci;

  // sanity check
  if (socket != NULL) {
    REprintf("Already connected to rabbitmq, close connection first\n");
    return ScalarLogical(FALSE);
  }

  // parse the URI
  amqp_parse_url(CHAR(asChar(uri)), &ci);

  // create connection
  conn = amqp_new_connection();

  socket = amqp_tcp_socket_new(conn);
  if (!socket) {
    REprintf("Error creating TCP socket\n");
    socket = NULL;
    return ScalarLogical(FALSE);
  }

  // Enable SSL (not tested)
  if (ci.ssl == 1) {
    amqp_ssl_socket_set_verify_peer(socket, 1);
    amqp_ssl_socket_set_verify_hostname(socket, 1);
  }

  int status = amqp_socket_open(socket, ci.host, ci.port);
  if (status) {
    REprintf("Error opening TCP socket\n");
    socket = NULL;
    return ScalarLogical(FALSE);
  }

  amqp_rpc_reply_t reply = amqp_login(conn, ci.vhost, 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, ci.user, ci.password);
  if (!amqp_check_status(reply, "Login")) {
    socket = NULL;
    return ScalarLogical(FALSE);
  }

  // open channel (only 1)
  amqp_channel_open(conn, 1);
  if (!amqp_check_status(amqp_get_rpc_reply(conn), "Open channel")) {
    socket = NULL;
    return ScalarLogical(FALSE);
  }

  return ScalarLogical(TRUE);
}

// ----------------------------------------------------------------------
// Close connection to RabbitMQ
// ----------------------------------------------------------------------
SEXP rabbitmq_close() {
  // sanity check
  if (socket == NULL) {
    REprintf("No open connection to rabbitmq\n");
    return ScalarLogical(FALSE);
  }
  socket = NULL;

  amqp_rpc_reply_t reply = amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS);
  if (!amqp_check_status(reply, "Closing channel")) {
    return ScalarLogical(FALSE);
  }

  reply = amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
  if (!amqp_check_status(reply, "Closing connection")) {
    return ScalarLogical(FALSE);
  }


  if (!amqp_check_error(amqp_destroy_connection(conn), "Ending connection")) {
    return ScalarLogical(FALSE);
  }

  return ScalarLogical(TRUE);
}

// ----------------------------------------------------------------------
// Create a queue in rabbitmq
// ----------------------------------------------------------------------
SEXP rabbitmq_create_queue(SEXP queue) {
  // sanity check
  if (socket == NULL) {
    REprintf("No open connection to rabbitmq\n");
    return ScalarLogical(FALSE);
  }

  // declare queue
  const char* queue_s = CHAR(asChar(queue));
  amqp_queue_declare_ok_t *res = amqp_queue_declare(conn, 1,
                                                    amqp_cstring_bytes(queue_s),
                                                    0, 1, 0, 0,
                                                    amqp_empty_table);
  if (res == NULL) {
    amqp_check_status(amqp_get_rpc_reply(conn), "queue.declare");
    return ScalarLogical(FALSE);
  }

  return ScalarLogical(TRUE);
}
// ----------------------------------------------------------------------
// destroy a queue in rabbitmq
// ----------------------------------------------------------------------
SEXP rabbitmq_destroy_queue(SEXP queue) {
  // sanity check
  if (socket == NULL) {
    REprintf("No open connection to rabbitmq\n");
    return ScalarLogical(FALSE);
  }

  // destroy queue
  const char* queue_s = CHAR(asChar(queue));
  amqp_queue_delete_ok_t *res = amqp_queue_delete(conn, 1,
                                                  amqp_cstring_bytes(queue_s),
                                                  0, 0);
  if (res == NULL) {
    amqp_check_status(amqp_get_rpc_reply(conn), "queue.delete");
    return ScalarLogical(FALSE);
  }

  return ScalarLogical(TRUE);
}

// ----------------------------------------------------------------------
// Publish a message to RabbitMQ
// ----------------------------------------------------------------------
SEXP rabbitmq_publish(SEXP queue, SEXP msg) {
  // sanity check
  if (socket == NULL) {
    REprintf("No open connection to rabbitmq\n");
    return ScalarLogical(FALSE);
  }

  // publish message
  const char* msg_s = CHAR(asChar(msg));
  const char* queue_s = CHAR(asChar(queue));
  amqp_basic_properties_t props;
  props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
  props.content_type = amqp_cstring_bytes("text/plain");
  props.delivery_mode = 2; // persistent delivery mode
  int reply = amqp_basic_publish(conn, 1,
                                 amqp_cstring_bytes(""),
                                 amqp_cstring_bytes(queue_s),
                                 0, 0, &props,
                                 amqp_cstring_bytes(msg_s));

  return ScalarLogical(amqp_check_error(reply, "Publish"));
}

// ----------------------------------------------------------------------
// Listen for message from RabbitMQ
// ----------------------------------------------------------------------
SEXP rabbitmq_read_message(SEXP queue, SEXP timeout) {
  // sanity check
  if (socket == NULL) {
    REprintf("No open connection to rabbitmq\n");
    return ScalarLogical(FALSE);
  }

  // listen for messages
  const char*  queue_s = CHAR(asChar(queue));
  amqp_basic_consume(conn, 1,
                     amqp_cstring_bytes(queue_s),
                     amqp_empty_bytes, 0, 0, 0,
                     amqp_empty_table);
  if (!amqp_check_status(amqp_get_rpc_reply(conn), "Consuming")) {
    return ScalarLogical(FALSE);
  }

  // read a message
  struct timeval tv = {1, 0};
  int count = asInteger(timeout);

  do {
    R_CheckUserInterrupt();

    amqp_envelope_t envelope;
    amqp_maybe_release_buffers(conn);
    amqp_rpc_reply_t reply = amqp_consume_message(conn, &envelope, &tv, 0);
    if (AMQP_RESPONSE_NORMAL == reply.reply_type) {
      // convert message to R
      char body[envelope.message.body.len + 1];
      sprintf(body, "%.*s", (int)envelope.message.body.len, (char *)envelope.message.body.bytes);
      SEXP out = PROTECT(allocVector(VECSXP, 2));
      SET_VECTOR_ELT(out, 0, PROTECT(ScalarInteger(envelope.delivery_tag)));
      SET_VECTOR_ELT(out, 1, PROTECT(mkString(body)));

      amqp_destroy_envelope(&envelope);

      UNPROTECT(3);
      return out;
    }
    if (AMQP_STATUS_TIMEOUT != reply.library_error) {
      amqp_check_status(reply, "consume.message");
      return ScalarLogical(FALSE);
    }

    if (!isNull(timeout)) {
      count -= 1;
    }
  } while(isNull(timeout) || count > 0);

  return ScalarLogical(FALSE);
}

// ----------------------------------------------------------------------
// Acknowledge message from RabbitMQ
// ----------------------------------------------------------------------
SEXP rabbitmq_ack_message(SEXP tag) {
  // sanity check
  if (socket == NULL) {
    REprintf("No open connection to rabbitmq\n");
    return ScalarLogical(FALSE);
  }

  int reply = amqp_basic_ack(conn, 1, asInteger(tag), 0);
  return ScalarLogical(amqp_check_error(reply, "ACK"));
}

// ----------------------------------------------------------------------
// Listen for message from RabbitMQ
// ----------------------------------------------------------------------
SEXP rabbitmq_nack_message(SEXP tag) {
  // sanity check
  if (socket == NULL) {
    REprintf("No open connection to rabbitmq\n");
    return ScalarLogical(FALSE);
  }

  int reply = amqp_basic_nack(conn, 1, asInteger(tag), 0, 1);
  return ScalarLogical(amqp_check_error(reply, "ACK"));
}
