package pl.edu.pw.clostream;

import java.io.Serializable;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * CloStream implementation based on the original publication
 * Yen, Show-Jane, et al. "An efficient algorithm for maintaining frequent closed itemsets over data stream." Next-Generation Applied Intelligence (2009): 767-776.
 * http://www.philippe-fournier-viger.com/spmf/yen2009.pdf
 *
 * with additional Closed Itemset Reduction procedure allowing easy parallelization
 * @author Maciej Majewski
 *
 * @param <T> item type
 */
public class CloStream<T extends Comparable<T>> implements Serializable {
    private Map<Collection<T>, Integer> closuresTable = new HashMap<>();
    private Map<T, Set<Collection<T>>> itemRelatedClosures = new HashMap<>();
    private Collection<T> emptyClosure;
    private transient Supplier<Collection<T>> collectionSupplier;

    private Collection<T> newCollection() {
        return collectionSupplier.get();
    }

    public CloStream() {
        this(HashSet::new);
    }

    /**
     * Changing the fundamental collection from default order- and duplicate-insensitive HashSet
     * may allow Closed Itemset mining taking duplicates and item order into consideration.
     *
     * Preliminary verification showed that using order- and duplicate sensitive collection like an
     * ArrayList, produces different results than the referencial implementation of CloStream from
     * SPMF library, therefore is not recommended at the moment.
     * http://www.philippe-fournier-viger.com/spmf/
     *
     * @param collectionSupplier
     */
    public CloStream(Supplier<Collection<T>> collectionSupplier) {
        this.collectionSupplier = collectionSupplier;
        emptyClosure = newCollection();
        closuresTable.put(emptyClosure, 0);
    }

    private Set<Collection<T>> getClosureIdSetForTransaction(Collection<T> transaction){
        Set<Collection<T>> result = new HashSet<>();
//        return itemRelatedClosures.entrySet().stream().filter(e -> transaction.contains(e.getKey())).flatMap(e -> e.getValue().stream()).collect(Collectors.toSet());
        transaction.stream().map(item -> itemRelatedClosures.get(item)).filter(Objects::nonNull).forEach(result::addAll);
        return result;
    }

    /**
     * Refactored implementation of pseudocode from the original publication
     * with the following changes:
     * - empty intersections are not added to the Temp table
     * - fixed closure Id substitution in the Temp table to match the given example (typo in pseudocode, description is correct)
     * @param rawTransaction
     */
    public void processNextTransaction(Collection<T> rawTransaction){
        Collection<T> transaction = collectionSupplier.get();//collection type consistency is needed
        transaction.addAll(rawTransaction);

        Map<Collection<T>, Collection<T>> temp = processNextItemsetPhase1(transaction);
        processNextItemsetPhase2(transaction, temp, 1);
    }
    protected Map<Collection<T>, Collection<T>> processNextItemsetPhase1(Collection<T> transaction){
        Map<Collection<T>, Collection<T>> temp = new HashMap<>();
        temp.put(transaction, emptyClosure);
        for(Collection<T> closureItemset : getClosureIdSetForTransaction(transaction)){
            Collection<T> intersection = collectionSupplier.get();
            intersection.addAll(closureItemset);
            intersection.retainAll(transaction);
            Collection<T> tempClosure = temp.get(intersection);
            if(tempClosure!=null){
                Integer tempSupport = closuresTable.get(tempClosure);
                Integer closureSupport = closuresTable.get(closureItemset);
                if(tempSupport == null || (closureSupport != null && closureSupport > tempSupport)){
                    temp.put(intersection, closureItemset);//fixed closure Id substitution in the Temp table to match the given example (typo in pseudocode, description is correct)
                }
            }else if(!intersection.isEmpty()){//empty intersections are not added to the Temp table
                temp.put(intersection, closureItemset);
            }
        }
        return temp;
    }
    protected void processNextItemsetPhase2(Collection<T> transaction, Map<Collection<T>, Collection<T>> temp, Integer increment){
        for(Map.Entry<Collection<T>, Collection<T>> tempEntry : temp.entrySet()){
            Collection<T> newItemset = tempEntry.getKey();
            Collection<T> closureItemset = tempEntry.getValue();

            Integer closureSupport = closuresTable.get(closureItemset);
            if(closureSupport == null){
                closureSupport = 0;
            }

            closuresTable.put(newItemset, closureSupport + increment);
            if(!closureItemset.equals(newItemset)){
                for(T item : transaction){
                    if(newItemset.contains(item)) {//fix?
                        Set<Collection<T>> relatedClosureSet = itemRelatedClosures.get(item);
                        if (relatedClosureSet == null) {
                            relatedClosureSet = new HashSet<>();
                        }
                        relatedClosureSet.add(newItemset);
                        itemRelatedClosures.put(item, relatedClosureSet);
                    }
                }
            }
        }
    }

    public Map<Collection<T>, Integer> getClosuresTable() {
        return closuresTable;
    }

    public Map<T, Set<Collection<T>>> getItemRelatedClosures() {
        return itemRelatedClosures;
    }

    protected Collection<T> getEmptyClosure() {
        return emptyClosure;
    }

    /**
     * Closed Itemset Reduction procedure implementation
     * @author Maciej Majewski
     *
     * @return reduced itemset
     */
    public Map<Collection<T>, Integer> getReducedClosuresTable(){
        Map<Collection<T>, Integer> reduced = new HashMap<>(this.getClosuresTable());
        Map<Collection<T>, Integer> result = new HashMap<>();

        while(true) {
            Map<Collection<T>, Integer> supersets = reduced.entrySet().stream()
                    .filter(entry -> reduced.keySet().stream()
                            .filter(agentClosure -> !agentClosure.equals(entry.getKey()))
                            .noneMatch(agentClosure -> agentClosure.containsAll(entry.getKey())))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            if(supersets.isEmpty()){
                break;
            }
            result.putAll(supersets);
            reduced.entrySet().removeIf(entry -> supersets.containsKey(entry.getKey()));
            reduced.entrySet().stream()
                    .filter(entry -> supersets.keySet().stream().anyMatch(se -> se.containsAll(entry.getKey())))
                    .forEach(entry ->
                            entry.setValue(entry.getValue() - supersets.entrySet().stream()
                                    .filter(se -> se.getKey().containsAll(entry.getKey()))
                                    .mapToInt(Map.Entry::getValue)
                                    .sum()));
            reduced.entrySet().removeIf(entry -> entry.getValue() <= 0);
        }

        return result;
    }

    public void trimClosures(Integer minSupport, Integer minSize){
        this.closuresTable.entrySet().removeIf(entry -> (minSupport != null && entry.getValue() < minSupport)||(minSize != null && entry.getKey().size() < minSize));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(Map.Entry<Collection<T>, Integer> closureTableEntry : closuresTable.entrySet()){
            sb.append("SC=").append(closureTableEntry.getValue()).append(", {");
            sb.append(closureTableEntry.getKey().stream().map(Object::toString).collect(Collectors.joining(", ")));
            sb.append("}\n");
        }
        return sb.toString();
    }
}
