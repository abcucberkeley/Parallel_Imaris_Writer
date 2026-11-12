# python -m parallel_imaris_writer <arguments>: the parallelimariswriter command line (also the
# parallelimariswriter console script).
import sys
from .parallel_imaris_writer_wrappers import run_cli


def main():
    sys.exit(run_cli(sys.argv[1:]))


if __name__ == '__main__':
    main()
