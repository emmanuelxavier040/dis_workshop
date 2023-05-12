package test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Test {

    public static void main(String[] args) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
        LocalDateTime l = LocalDateTime.parse("30/09/2021 23:56:11", formatter);
        System.out.print(l);
    }
}
