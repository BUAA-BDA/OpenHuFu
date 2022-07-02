nohup java -jar target/benchmarks.jar TPCHBenchmark -rf json 2>&1 &
echo $! > pid
