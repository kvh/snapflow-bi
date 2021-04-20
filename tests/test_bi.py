from loguru import logger

logger.enable("snapflow")


def test():
    from snapflow_bi import module as bi

    bi.run_tests()


if __name__ == "__main__":
    test()
