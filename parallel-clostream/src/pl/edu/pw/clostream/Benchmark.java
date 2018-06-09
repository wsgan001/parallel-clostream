package pl.edu.pw.clostream;

//import pl.edu.pw.instrumentation.ObjectSizeFetcher;

import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.security.SecureRandom;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Benchmark {
    private SecureRandom RANDOM = new SecureRandom();
    private Integer MAX_TRANSACTION_LENGTH = 10;
    private Integer PRODUCTS = 100;
    private List<Integer> PRODUCT_LIST = new ArrayList<Integer>(){{
        for (int i = 0; i < PRODUCTS; i++) {
            add(i);
        }
    }};

    private Collection<Integer> randomItemset(){
        int transactionLength = RANDOM.nextInt(MAX_TRANSACTION_LENGTH)+1;
        ArrayList<Integer> items = new ArrayList<>();
        ArrayList<Integer> products = new ArrayList<>(PRODUCT_LIST);
        for (int i = 0; i < transactionLength; i++) {
            int p = RANDOM.nextInt(products.size());
            items.add(products.get(p));
            products.remove(p);
        }
        return items;
    }

    private List<Collection<Integer>> randomItemsetList(int length){
        ArrayList<Collection<Integer>> list = new ArrayList<Collection<Integer>>(length);
        for (int i = 0; i < length; i++) {
            list.add(randomItemset());
        }
        return list;
    }

    /**
     * Correctness test based on the original publication.
     * Requires VM option: -ea
     */
    public void correctnessTest(CloStream<String> c){
        Collection<String> transaction1 = new HashSet<>(Arrays.asList("C", "D"));
        Collection<String> transaction2 = new HashSet<>(Arrays.asList("A", "B"));
        Collection<String> transaction3 = new HashSet<>(Arrays.asList("A", "B", "C"));
        Collection<String> transaction4 = new HashSet<>(Arrays.asList("A", "B", "C"));
        Collection<String> transaction5 = new HashSet<>(Arrays.asList("A", "C", "D"));
        Collection<String> transaction6 = new HashSet<>(Arrays.asList("B", "C"));

        c.processNextTransaction(transaction1);
        c.processNextTransaction(transaction2);
        c.processNextTransaction(transaction3);
        c.processNextTransaction(transaction4);
        c.processNextTransaction(transaction5);

        assert c.getClosuresTable().size() == 8;
        assert c.getClosuresTable().keySet().stream().filter(e -> e.equals(new HashSet<>())).allMatch(e -> c.getClosuresTable().get(e).equals(0));
        assert c.getClosuresTable().keySet().stream().filter(e -> e.equals(new HashSet<>(Arrays.asList("C", "D")))).allMatch(e -> c.getClosuresTable().get(e).equals(2));
        assert c.getClosuresTable().keySet().stream().filter(e -> e.equals(new HashSet<>(Arrays.asList("A", "B")))).allMatch(e -> c.getClosuresTable().get(e).equals(3));
        assert c.getClosuresTable().keySet().stream().filter(e -> e.equals(new HashSet<>(Arrays.asList("A", "B", "C")))).allMatch(e -> c.getClosuresTable().get(e).equals(2));
        assert c.getClosuresTable().keySet().stream().filter(e -> e.equals(new HashSet<>(Arrays.asList("C")))).allMatch(e -> c.getClosuresTable().get(e).equals(4));
        assert c.getClosuresTable().keySet().stream().filter(e -> e.equals(new HashSet<>(Arrays.asList("A", "C", "D")))).allMatch(e -> c.getClosuresTable().get(e).equals(1));
        assert c.getClosuresTable().keySet().stream().filter(e -> e.equals(new HashSet<>(Arrays.asList("A")))).allMatch(e -> c.getClosuresTable().get(e).equals(4));
        assert c.getClosuresTable().keySet().stream().filter(e -> e.equals(new HashSet<>(Arrays.asList("A", "C")))).allMatch(e -> c.getClosuresTable().get(e).equals(3));

        c.processNextTransaction(transaction6);

        assert c.getClosuresTable().size() == 10;
        assert c.getClosuresTable().keySet().stream().filter(e -> e.equals(new HashSet<>())).allMatch(e -> c.getClosuresTable().get(e).equals(0));
        assert c.getClosuresTable().keySet().stream().filter(e -> e.equals(new HashSet<>(Arrays.asList("C", "D")))).allMatch(e -> c.getClosuresTable().get(e).equals(2));
        assert c.getClosuresTable().keySet().stream().filter(e -> e.equals(new HashSet<>(Arrays.asList("A", "B")))).allMatch(e -> c.getClosuresTable().get(e).equals(3));
        assert c.getClosuresTable().keySet().stream().filter(e -> e.equals(new HashSet<>(Arrays.asList("A", "B", "C")))).allMatch(e -> c.getClosuresTable().get(e).equals(2));
        assert c.getClosuresTable().keySet().stream().filter(e -> e.equals(new HashSet<>(Arrays.asList("C")))).allMatch(e -> c.getClosuresTable().get(e).equals(5));
        assert c.getClosuresTable().keySet().stream().filter(e -> e.equals(new HashSet<>(Arrays.asList("A", "C", "D")))).allMatch(e -> c.getClosuresTable().get(e).equals(1));
        assert c.getClosuresTable().keySet().stream().filter(e -> e.equals(new HashSet<>(Arrays.asList("A")))).allMatch(e -> c.getClosuresTable().get(e).equals(4));
        assert c.getClosuresTable().keySet().stream().filter(e -> e.equals(new HashSet<>(Arrays.asList("A", "C")))).allMatch(e -> c.getClosuresTable().get(e).equals(3));
        assert c.getClosuresTable().keySet().stream().filter(e -> e.equals(new HashSet<>(Arrays.asList("B")))).allMatch(e -> c.getClosuresTable().get(e).equals(4));
        assert c.getClosuresTable().keySet().stream().filter(e -> e.equals(new HashSet<>(Arrays.asList("B", "C")))).allMatch(e -> c.getClosuresTable().get(e).equals(3));
    }

    public void feedRandomTest(List<Collection<Integer>> batchData, Set<CloStream<Integer>> agentSet) {
        int agentCounter = 0;
        Map<Integer, CloStream<Integer>> agentIdMap = new HashMap<>(agentSet.size());
        for (CloStream<Integer> agent : agentSet) {
            agentIdMap.put(agentCounter++, agent);
        }

        int transactionCounter=0;
        for(Collection<Integer> transaction : batchData){
            agentIdMap.get(transactionCounter++%agentCounter).processNextTransaction(transaction);
            if(transactionCounter%100 == 0){
                for(CloStream<Integer> agent : agentSet){
                    agent.trimClosures(2,2);
                }
            }
        }

    }

    public List<Integer> listErrors(Map<Collection<Integer>, Integer> loneAgentClosures, Map<Collection<Integer>, Integer> aggregatedClosures){
        List<Integer> errors = new ArrayList<>();
        for(Map.Entry<Collection<Integer>, Integer> closureTableEntry : aggregatedClosures.entrySet()){
            Integer error = closureTableEntry.getValue() - loneAgentClosures.get(closureTableEntry.getKey());
            if(error != 0) {
                errors.add(error);
            }
        }
        return errors;
    }

    public int calculateObjectSize(Object obj) throws IOException {

        ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteOutputStream);

        objectOutputStream.writeObject(obj);
        objectOutputStream.flush();
        objectOutputStream.close();

        return byteOutputStream.toByteArray().length;
    }

    public void runBenchmark(LoggingQueues out, int testId, int agentsAmount, int batchLength) throws IOException, InterruptedException {

        CloStream<Integer> loneAgent = new CloStream<>();

        CloStreamParallelAggregator<Integer> aggregator = new CloStreamParallelAggregator<>();
        IntStream.range(0, agentsAmount).<CloStream<Integer>>mapToObj(i -> new CloStream<>()).forEach(aggregator::registerCloStreamAgent);

        List<Collection<Integer>> batchData = randomItemsetList(batchLength);

        long loneTestFeedTime = System.currentTimeMillis();
            feedRandomTest(batchData, Collections.singleton(loneAgent));
            loneAgent.trimClosures(2,2);
        loneTestFeedTime = System.currentTimeMillis() - loneTestFeedTime;

        long aggregatedTestFeedTime = System.currentTimeMillis();
            feedRandomTest(batchData, aggregator.getAgents());
            for(CloStream<Integer> agent : aggregator.getAgents()){
                agent.trimClosures(2,2);
            }
        aggregatedTestFeedTime = System.currentTimeMillis() - aggregatedTestFeedTime;

        long aggregationTime = System.currentTimeMillis();
            aggregator.aggregate();
            aggregator.trimClosures(2,2);
        aggregationTime = System.currentTimeMillis() - aggregationTime;

        long loneAgentMemorySize = calculateObjectSize(loneAgent);//ObjectSizeFetcher.getObjectSize(loneAgent);
        long aggregatedAgentMemorySize = calculateObjectSize(aggregator);//ObjectSizeFetcher.getObjectSize(aggregator);

        long averageAgentClosureSize = 0;
        long averageAgentReductionTime = 0;
        long averageAgentMemorySize = 0;
        for(CloStream<Integer> agent : aggregator.getAgents()){
            long agentReductionTime = System.currentTimeMillis();
            int reducedTableSize = agent.getReducedClosuresTable().size();
            agentReductionTime = System.currentTimeMillis() - agentReductionTime;
            long agentMemorySize = calculateObjectSize(agent);//ObjectSizeFetcher.getObjectSize(agent);
            averageAgentClosureSize += reducedTableSize;
            averageAgentReductionTime += agentReductionTime;
            averageAgentMemorySize += agentMemorySize;
        }
        averageAgentClosureSize /= aggregator.getAgents().size();
        averageAgentReductionTime /= aggregator.getAgents().size();
        averageAgentMemorySize /= aggregator.getAgents().size();


        // TestId,BatchLength,AgentsAmount,
        // LoneTestFeedTime,AggregatedTestFeedTime,FeedTimeLoss,
        // AggregationTime,FeedTimeLossWithAggregation,
        // LoneAgentMemorySize,AggregatedAgentMemorySize,MemoryLoss,
        // AverageAgentClosuresTableSize,AverageAgentReductionTime,AverageAgentMemorySize
        out.getTests().put(Stream.of(
                testId, batchData.size(), agentsAmount,
                loneTestFeedTime, aggregatedTestFeedTime, aggregatedTestFeedTime - loneTestFeedTime,
                aggregationTime,  (aggregatedTestFeedTime+aggregationTime) - loneTestFeedTime,
                loneAgentMemorySize, aggregatedAgentMemorySize, aggregatedAgentMemorySize - loneAgentMemorySize,
                averageAgentClosureSize, averageAgentReductionTime, averageAgentMemorySize
        ).map(Object::toString).collect(Collectors.joining(",")));



        IntSummaryStatistics aggregatedErrorStats = listErrors(loneAgent.getClosuresTable(), aggregator.getClosuresTable()).stream().mapToInt(Math::abs).summaryStatistics();
        // TestId,LoneAgentClosuresAmount,AggregatedClosuresAmount,
        // AgentErrorsAmount,AgentMinError,AgentMaxError,AgentAverageError
        out.getErrors().put(Stream.of(
                testId, loneAgent.getClosuresTable().size(), aggregator.getClosuresTable().size(),
                aggregatedErrorStats.getCount(), aggregatedErrorStats.getMin(), aggregatedErrorStats.getMax(), aggregatedErrorStats.getAverage()
        ).map(Object::toString).collect(Collectors.joining(",")));
    }

    private static class LoggingQueues {
        private BlockingQueue<String> tests = new ArrayBlockingQueue<>(100);
//        private BlockingQueue<String> agents = new ArrayBlockingQueue<>(100);
        private BlockingQueue<String> errors = new ArrayBlockingQueue<>(100);

        public BlockingQueue<String> getTests() {
            return tests;
        }

//        public BlockingQueue<String> getAgents() {
//            return agents;
//        }
//
        public BlockingQueue<String> getErrors() {
            return errors;
        }
    }

    private static class BenchmarkTask implements Callable<Void> {
        private LoggingQueues loggingQueues;
        private int testId;
        private int agents;
        private int batchLength;
        private int repetition;

        public BenchmarkTask(LoggingQueues loggingQueues, int testId, int agents, int batchLength, int repetition) {
            this.loggingQueues = loggingQueues;
            this.testId = testId;
            this.agents = agents;
            this.batchLength = batchLength;
            this.repetition = repetition;
        }

        @Override
        public Void call() {
            System.out.println(String.format("[%tF %tT] Starting test %05d, agents=%02d, batch=%04d, repetition=%02d",
                    LocalDateTime.now(), LocalDateTime.now(), testId, agents, batchLength, repetition));

            Benchmark benchmark = new Benchmark();
            try {
                benchmark.runBenchmark(loggingQueues, ++testId, agents, batchLength);
            } catch (InterruptedException|IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    private static class LoggingTask implements Runnable {
        private FileWriter fileWriter;
        private BlockingQueue<String> queue;

        public LoggingTask(FileWriter fileWriter, BlockingQueue<String> queue) {
            this.fileWriter = fileWriter;
            this.queue = queue;
        }

        @Override
        public void run() {
            try {
                while(true) {
                    String msg = queue.take();
                    if("".equals(msg)){
                        fileWriter.close();
                        break;
                    }
                    fileWriter.append(msg).append(System.lineSeparator());
                }
            } catch (InterruptedException|IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        Integer threads = Integer.parseInt(args[0]);

        LoggingQueues loggingQueues = new LoggingQueues();
        LoggingTask testLoggingTask = new LoggingTask(new FileWriter("tests.csv", true), loggingQueues.getTests());
//        LoggingTask agentsLoggingTask = new LoggingTask(new FileWriter("agents.csv", true), loggingQueues.getAgents());
        LoggingTask errorsLoggingTask = new LoggingTask(new FileWriter("errors.csv", true), loggingQueues.getErrors());

        loggingQueues.getTests().put("TestId,BatchLength,AgentsAmount,LoneTestFeedTime,AggregatedTestFeedTime,FeedTimeLoss,AggregationTime,FeedTimeLossWithAggregation,LoneAgentMemorySize,AggregatedAgentMemorySize,MemoryLoss,AverageAgentClosuresTableSize,AverageAgentReductionTime,AverageAgentMemorySize");
//        loggingQueues.getTests().put("TestId,BatchLength,AgentsAmount,LoneTestFeedTime,AggregatedTestFeedTime,AggregationTime,LoneAgentClosuresAmount,AggregatedClosuresAmount,LoneAgentMemorySize,AggregatedAgentMemorySize");
//        loggingQueues.getAgents().put("TestId,AgentId,AgentClosuresTableSize,AgentReducedTableSize,AgentReductionTime,AgentMemorySize");
        loggingQueues.getErrors().put("TestId,LoneAgentClosuresAmount,AggregatedClosuresAmount,AgentErrorsAmount,AgentMinError,AgentMaxError,AgentAverageError");
        System.out.println(String.format("[%tF %tT] Preparing logging threads...",LocalDateTime.now(), LocalDateTime.now()));
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        Future<?> testLoggingFuture = executorService.submit(testLoggingTask);
//        Future<?> agentLoggingFuture = executorService.submit(agentsLoggingTask);
        Future<?> errorLoggingFuture = executorService.submit(errorsLoggingTask);

        System.out.println(String.format("[%tF %tT] Preparing tasks...",LocalDateTime.now(), LocalDateTime.now()));
        List<Callable<Void>> taskList = new ArrayList<>();
        int testId = 0;
        for (int batchLength = 500; batchLength <= 1000; batchLength+=100) {
            for (int agents = 2; agents <= 4 && agents <= batchLength; agents*=2) {
                for (int repetition = 0; repetition < 1; repetition++) {
                    taskList.add(new BenchmarkTask(
                            loggingQueues, ++testId, agents, batchLength, repetition
                    ));
                }
            }
        }

        System.out.println(String.format("[%tF %tT] Spinning up...",LocalDateTime.now(), LocalDateTime.now()));
        List<Future<Void>> benchmarkFutures = executorService.invokeAll(taskList);
        for(Future<Void> benchmarkFuture : benchmarkFutures){
            benchmarkFuture.get();
        }
        System.out.println(String.format("[%tF %tT] All tasks done!",LocalDateTime.now(), LocalDateTime.now()));

        loggingQueues.getTests().put("");
//        loggingQueues.getAgents().put("");
        loggingQueues.getErrors().put("");
        testLoggingFuture.get();
//        agentLoggingFuture.get();
        errorLoggingFuture.get();
        executorService.shutdown();
    }
}
