#' Connect to a rabbitmq host.
#'
#' This function will connect to a rabbitmq server. The default host
#' it will connect to is localhost. This function needs to be called
#' once before all other functions are called. The function will
#' return TRUE if the connection is created successfully. If already
#' connected or the connection failed to create it will return FALSE.
#'
#' @param host the hostname of the RabbitMQ server
#' @param port the hostname of the RabbitMQ server
#' @param vhost the hostname of the RabbitMQ server
#' @return TRUE if the connection was created or FALSE if no
#'         connection could be created.
#'
#' @examples
#' \dontrun{
#' stopifnot(rabbitmq_connect(host = "rabbitmq"))
#' }
#'
#' @export
#' @useDynLib rrabbitmq, .registration = TRUE
rabbitmq_connect <- function(host="localhost", port=5672, vhost="%2F") {
  .Call(c_rabbitmq_connect, host)
}

#' Close an open connection to a rabbitmq host.
#'
#' This function will close the connection to a rabbitmq server. The
#' The function will return TRUE if the connection is closed, if there
#' is no connection, or the connection could not be closed it will
#' return FALSE.
#'
#' @return TRUE if the connection was closed or FALSE if the
#'         connection could not be closed.
#'
#' @examples
#' \dontrun{
#' rabbitmq_close()
#' }
#'
#' @export
#' @useDynLib rrabbitmq, .registration = TRUE
rabbitmq_close <- function() {
  .Call(c_rabbitmq_close)
}

#' Create a queue
#'
#' This function will connect to a rabbitmq server. The default host
#' it will connect to is localhost. This function needs to be called
#' once before all other functions are called. The function will
#' return TRUE if the connection is created successfully. If already
#' connected or the connection failed to create it will return FALSE.
#'
#' @param host the hostname of the RabbitMQ server
#' @return TRUE if the connection was created or FALSE if no
#'         connection could be created.
#' @examples
#' \dontrun{
#' stopifnot(rabbitmq_connect(host = "rabbitmq"))
#' }
#'
#' @export
#' @useDynLib rrabbitmq, .registration = TRUE
rabbitmq_create_queue <- function(queue) {
  .Call(c_rabbitmq_create_queue, queue)
}

rabbitmq_destroy_queue <- function(queue) {
  .Call(c_rabbitmq_destroy_queue, queue)
}

rabbitmq_publish <- function(queue, message) {
  .Call(c_rabbitmq_publish, queue, message)
}

rabbitmq_read_message <- function(queue, timeout=NULL) {
  result <- .Call(c_rabbitmq_read_message, queue, timeout)
  if (is.logical(result) && !result) {
    return(list())
  } else {
    return(list(id=result[[1]], msg=result[[2]]))
  }
}

rabbitmq_listen <- function(queue, callback, timeout=NULL) {
  while(TRUE) {
    result <- rabbitmq_read_message(queue, timeout)
    if (length(result) == 0) {
      return()
    }
    callback_result = do.call(callback, list(result$msg))
    ack = (!is.logical(callback_result) || as.logical(callback_result))
    rabbitmq_ack_message(result$id, ack)
  }
}

rabbitmq_ack_message <- function(id, ack=TRUE) {
  if(ack) {
    .Call(c_rabbitmq_ack_message, id)
  } else {
    .Call(c_rabbitmq_nack_message, id)
  }
}
