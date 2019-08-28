import erasure.ReedSolomon;

public class Main {
    public static void main(String[] args) {
        System.out.println("hello world");
        ReedSolomon reedSolomon = new ReedSolomon(4, 2);
        int dataShardCount = reedSolomon.getDataShardCount();
        System.out.println(dataShardCount);
    }
}
