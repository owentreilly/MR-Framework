# MR-Framework
MapReduce Framework written in GoLang. Also includes several plugin applications that work on top of the framwork such as word count application, indexing and various other programs. Currently uses unix sockets on a local machine but can easily be changed to run over different machines using tcp sockets. 

designed to be compiled and run from the command line. in Unix use go build -buildmode=plugin ../mr-main/mrapps/wc.go, or other filenames. Run a coordinator using go run go run mrcoordinator.go ../data/pg-*.txt with any data in the working directory.
