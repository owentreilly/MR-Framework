# MR-Framework
MapReduce Framework written in Go. Also includes several plugin applications that work on top of the framework such as word count application, indexing and various other programs. Currently uses unix sockets on a local machine but can easily be changed to run over different machines using tcp sockets. Includes coordinator files, worker files and rpc descriptions in rpc.go.

designed to be compiled and run from the command line. in Unix use go build -buildmode=plugin ../mr-main/mrapps/wc.go, or other filenames. Run a coordinator using go run go run mrcoordinator.go ../data/pg-*.txt with any data in the working directory. Then run several workers go run mrworker.go <pluginFileName> to recieve tasks from the coordinator. 
