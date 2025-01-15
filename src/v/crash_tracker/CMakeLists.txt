v_cc_library(
  NAME crash_tracker
  SRCS
    logger.cc
  DEPS
    Seastar::seastar
    v::base
    v::model
    v::serde
)
