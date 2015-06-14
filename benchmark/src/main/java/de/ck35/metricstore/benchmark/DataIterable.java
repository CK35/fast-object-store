package de.ck35.metricstore.benchmark;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;

import org.joda.time.DateTime;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Supplier;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Maps;

public class DataIterable implements Iterable<Entry<BucketInfo, ObjectNode>> {

    private final Random random;
    private final Supplier<List<Entry<BucketInfo, List<Entry<DateTime, ObjectNode>>>>> dataSupplier;

    public DataIterable(Random random, Supplier<List<Entry<BucketInfo, List<Entry<DateTime, ObjectNode>>>>> dataSupplier) {
        this.random = random;
        this.dataSupplier = dataSupplier;
    }

    @Override
    public Iterator<Entry<BucketInfo, ObjectNode>> iterator() {
        return DataIterator.build(random, dataSupplier.get());
    }
    
    public static class DataIterator extends AbstractIterator<Entry<BucketInfo, ObjectNode>> {

        private final Random random;
        private final List<Entry<BucketInfo, Iterator<Entry<DateTime, ObjectNode>>>> iterators;
        
        public DataIterator(Random random, List<Entry<BucketInfo, Iterator<Entry<DateTime, ObjectNode>>>> iterators) {
            this.random = random;
            this.iterators = iterators;
        }
        public static DataIterator build(Random random, List<Entry<BucketInfo, List<Entry<DateTime, ObjectNode>>>> list) {
            List<Entry<BucketInfo, Iterator<Entry<DateTime, ObjectNode>>>> iterators = new ArrayList<>(list.size());
            for(Entry<BucketInfo, List<Entry<DateTime, ObjectNode>>> entry : list) {
                iterators.add(Maps.immutableEntry(entry.getKey(), entry.getValue().iterator()));
            }
            return new DataIterator(random, iterators);
        }
        @Override
        protected Entry<BucketInfo, ObjectNode> computeNext() {
            while(!iterators.isEmpty()) {                
                int index = random.nextInt(iterators.size());
                Entry<BucketInfo, Iterator<Entry<DateTime, ObjectNode>>> entry = iterators.get(index);
                if(entry.getValue().hasNext()) {
                    return Maps.immutableEntry(entry.getKey(), entry.getValue().next().getValue());
                } else {
                    iterators.remove(index);
                }
            }
            return endOfData();
        }
    }
}