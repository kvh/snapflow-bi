from loguru import logger

logger.enable("snapflow")


def test():
    import snapflow_bi

    snapflow_bi.run_tests()


if __name__ == "__main__":
    test()
