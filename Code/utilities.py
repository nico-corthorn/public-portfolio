
from concurrent import futures


def compute(args, fun, max_workers=6):
    """ General purpose parallel computing function. """
    print("\nProcessing symbols in parallel")
    ex = futures.ThreadPoolExecutor(max_workers=max_workers)
    ex.map(fun, args)


def compute_loop(args, fun):
    """ For debugging purposes. """
    print("\nProcessing symbols one by one")
    for symbol in args:
        fun(symbol)