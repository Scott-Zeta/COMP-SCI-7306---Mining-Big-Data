//package com.analysis.mapreduce;
import java.io.*;
import java.util.*;

public class PageRank {
    /**
     * damping factor
     */
    private static final double D = 0.85;
    private static final int iterationsNum = -1;
    public static int iterationsRound = 0;

    public static void main(String[] args) throws IOException {

        if (args.length != 2) {
            System.out.println("请输入 <input> <output>");
            System.exit(-1);
        }
        String inputFilePath = args[0];
        String outputFilePath = args[1];


        BufferedReader br = new BufferedReader(new FileReader(inputFilePath));
        String line;
        HashMap<Integer, List<Integer>> sourceData = new HashMap<>();
        while ((line = br.readLine()) != null) {
            if (line.startsWith("#")) {
                continue;
            }
            String[] split = line.split("\t");
            Integer fromPageId = Integer.parseInt(split[0].trim());
            Integer toPageId = Integer.parseInt(split[1].trim());
            List<Integer> toPageIdList = sourceData.get(fromPageId);
            if (Objects.isNull(toPageIdList)) {
                toPageIdList = new ArrayList<>();
                sourceData.put(fromPageId, toPageIdList);
            }
            toPageIdList.add(toPageId);
        }

        System.out.println("fromPageId num: " + sourceData.size());

        HashMap<Integer, HashMap<Integer, Double>> matrix = new HashMap<>();
        for (Map.Entry<Integer, List<Integer>> entry : sourceData.entrySet()) {
            Integer fromPageId = entry.getKey();
            List<Integer> toPageIdList = entry.getValue();
            toPageIdList.sort(Comparator.comparingInt(o -> o));
            for (Integer toPageId : toPageIdList) {
                HashMap<Integer, Double> intoChainList = matrix.get(toPageId);
                if (Objects.isNull(intoChainList)) {
                    intoChainList = new HashMap<>();
                    matrix.put(toPageId, intoChainList);
                }
                intoChainList.put(fromPageId, (double) 1 / (toPageIdList.size()));
            }
            if (!matrix.containsKey(fromPageId)) {
                matrix.put(fromPageId, new HashMap<>());
            }
        }
        sourceData = null;

        System.out.println("The amount of the Nodes provided is correct? " + (matrix.size() == 875713));

        // initialize the pr value
        double pr = (double) 1 / matrix.size();
        System.out.println("initial PR: " + pr);
        HashMap<Integer, Double> prMap = new HashMap<>();
        for (Integer toPageId : matrix.keySet()) {
            prMap.put(toPageId, pr);
        }


        if (iterationsNum > 0) {
            for (int i = 0; i < iterationsNum; i++) {
                prMap = calculatePR(matrix, prMap);
                System.out.println("iteration " + i);
            }
        } else {
            while (true) {
                iterationsRound++;
                HashMap<Integer, Double> newPrMap = calculatePR(matrix, prMap);
                double distance = calculateEuclideanDistance(newPrMap, prMap);
                System.out.printf("iterations round: %d, iteration new pr and old pr distance:%f \n", iterationsRound, distance);
                if (distance < 0.000001) {
                    break;
                }
                prMap = newPrMap;
            }
        }

        List<Map.Entry<Integer, Double>> result = new ArrayList<>(prMap.entrySet());
        result.sort((o1, o2) -> -o1.getValue().compareTo(o2.getValue()));
        System.out.println(result.size());
        writeFile(outputFilePath, result);
    }

    private static HashMap<Integer, Double> calculatePR(HashMap<Integer, HashMap<Integer, Double>> matrix, HashMap<Integer, Double> prValue) {
        HashMap<Integer, Double> updatedPRV = new HashMap<>();
        for (Map.Entry<Integer, HashMap<Integer, Double>> entry : matrix.entrySet()) {
            Integer toPageID = entry.getKey();
            HashMap<Integer, Double> fromPageIDMap = entry.getValue();
            double newPR = 0;
            for (Map.Entry<Integer, Double> integerDoubleEntry : fromPageIDMap.entrySet()) {
                Integer fromPageID = integerDoubleEntry.getKey();
                Double valInM = integerDoubleEntry.getValue();
                if (!matrix.get(toPageID).isEmpty()) {
                    double valInR = prValue.get(fromPageID);
                    newPR += D * valInR * valInM;
                }
            }
            newPR += (1 - D) / matrix.size();
            updatedPRV.put(toPageID, newPR);
        }
        return updatedPRV;
    }

    private static double calculateEuclideanDistance(HashMap<Integer, Double> PRValue1, HashMap<Integer, Double> PRValue2) {
        double sumDistance = 0;
        for (Map.Entry<Integer, Double> entry : PRValue1.entrySet()) {
            Integer key = entry.getKey();
            Double value = entry.getValue();
            sumDistance += Math.pow(value - PRValue2.get(key), 2);
        }
        return Math.sqrt(sumDistance);
    }

    private static void writeFile(String path, List<Map.Entry<Integer, Double>> result) throws IOException {
        File output = new File(path);
        if (!output.exists()) {
            output.createNewFile();
        }
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(output, false));

        StringJoiner answer = new StringJoiner(", ");
        for (int i = 0; i < result.size(); i++) {
            Map.Entry<Integer, Double> entry = result.get(i);
            bufferedWriter.write(entry.getKey() + ": " + entry.getValue() + "\n");
            if (i <= 9) {
                answer.add((i + 1) + " " + entry.getKey() + ": " + entry.getValue());
            }
        }
        bufferedWriter.close();
        System.out.printf("The answer is :%s", answer);
    }
}