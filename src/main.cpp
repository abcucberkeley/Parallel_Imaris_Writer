// parallelimariswriter: the command line of src/cli.cpp (the conversion is piw::convert in src/converter.cpp).
#include "parallelimariswriter.h"
#include <signal.h>

int main(int argc, char** argv){
#ifdef SIGXFSZ
    // Past a file-size limit (ulimit -f) a write then fails (EFBIG) and the conversion ends with an
    // error and removes its partial output, instead of the signal ending the process (Python ignores
    // it too).
    signal(SIGXFSZ, SIG_IGN);
#endif
    return piw::runCli(argc, argv);
}
