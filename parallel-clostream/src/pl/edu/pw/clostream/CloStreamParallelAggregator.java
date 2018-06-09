package pl.edu.pw.clostream;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Parallel CloStream implementation, heavily dependant on the original.
 * @author Maciej Majewski
 *
 * @param <T> item type
 */
public class CloStreamParallelAggregator<T extends Comparable<T>> extends CloStream<T>  implements Serializable {
    private Set<CloStream<T>> agents = new HashSet<>();

    public CloStreamParallelAggregator() {
    }

    public CloStreamParallelAggregator(Supplier<Collection<T>> collectionSupplier) {
        super(collectionSupplier);
    }

    public CloStreamParallelAggregator<T> registerCloStreamAgent(CloStream<T> agent){
        agents.add(agent);
        return this;
    }

    public CloStream<T> aggregate(){
        this.getClosuresTable().clear();
        this.getItemRelatedClosures().clear();
        for(CloStream<T> agent : agents){
            for(Map.Entry<Collection<T>, Integer> closedEntry : agent.getReducedClosuresTable().entrySet()){
                Collection<T> itemset = closedEntry.getKey();
                if(!itemset.isEmpty()) {
                    Map<Collection<T>, Collection<T>> temp = this.processNextItemsetPhase1(itemset);
                    this.processNextItemsetPhase2(itemset, temp, closedEntry.getValue());
                }
            }
        }
        this.getClosuresTable().put(this.getEmptyClosure(), 0);
        return this;
    }

    public Set<CloStream<T>> getAgents() {
        return agents;
    }
}
