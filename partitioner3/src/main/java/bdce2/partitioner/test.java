package bdce2.partitioner;

import java.util.*;
import java.util.Map.Entry;

//A trial java program to see how sorting works in Hasmap.

public class test {

public static Map<String, Integer> sortByValueDesc(Map<String, Integer> map) {
    List<Map.Entry<String, Integer>> list = new LinkedList<Entry<String, Integer>>(map.entrySet());
    Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
        public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
            return o2.getValue().compareTo(o1.getValue());
        }
    });

    Map<String, Integer> result = new LinkedHashMap<String, Integer>();
    for (Map.Entry<String, Integer> entry : list) {
        result.put(entry.getKey(), entry.getValue());
    }
    return result;
}

    public static void main(String[] args) {

        HashMap<String, Integer> map = new HashMap<String, Integer>();

        map.put("item1", 1);
        map.put("item2", 2);
        map.put("item3", 7);
        map.put("item4", 7);
        map.put("item5", 3);
        map.put("item6", 4);

        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            System.out.println("Item is:" + entry.getKey() + " with value:"
                    + entry.getValue());
        }

        System.out.println("*******");

        Map<String,Integer> sortedMap = sortByValueDesc(map);

        for (Map.Entry<String, Integer> entry : sortedMap.entrySet()) {
            System.out.println("Item is:" + entry.getKey() + " with value:"
                    + entry.getValue());
        }

    }

}