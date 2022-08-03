import java.io.*;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class fileReaderTask {
    public static void main(String[] args) throws FileNotFoundException {
    fileReaderTask read = new fileReaderTask();
    read.readFile("C:\\file\\txnlog.dat");

    }

    public void readFile(String address) {
        // variables declaration and initialisation.
        double creditAmount = 0;
        double debitAmount = 0;
        int startAutopay = 0;
        int endAutopay = 0;
        double balanceForUser2456938384156277127 = 0;
        // used for rounding double and floating values.
        DecimalFormat df = new DecimalFormat("0.00");


        try {

            // file reading used dataInputStream and FileReader.
            DataInputStream input = new DataInputStream(new FileInputStream(
                    address));
            Reader inputReader = new FileReader(address);

            // used to get size of file.
            int count = input.available();
            // this array will be used to read data from file.
            char[] array = new char[count];

            // encoded file is being read here.
            inputReader.read(array);

            // need offset to iterate through the file to do operations, to convert file into readable form.
            int offset = 0;

            // header text is being extracted here.
            String header = "";
            for (int i = offset; i < offset + 4; i++) {
                header += array[i];

            }
            offset += 4;

            // version is being extracted here.
            String version = "";
            for (int i = offset; i < offset + 1; i++) {
                version += array[i];

            }
            offset += 1;

            // no of records is being extracted here.
            String noOfRecords = "";
            for (int i = offset; i < offset + 4; i++) {
                noOfRecords += array[i];

            }
            offset += 2;

            // header conversion
            byte[] headerBytes = header.getBytes(StandardCharsets.UTF_8);
            String headerText = new String(headerBytes, StandardCharsets.UTF_8);
            System.out.println("header:: " + headerText);

            // version conversion
            byte[] versionBytes = version.getBytes();
            String versionText = "";
            versionText = bytesToHex(versionBytes);
            System.out.println("versionText ::" + versionText);


            // no of records conversion
            byte[] noOfRecordsBytes = noOfRecords.getBytes();
            int loffset = 2;
           // System.out.println("noOfRecordsText ::" + byteArrayToUInt32(noOfRecordsBytes, loffset));

            // loop to read records
            while (offset < count) {

                // extraction of record type
                String recordType = "";
                for (int i = offset; i < offset + 1; i++) {
                    if (offset + 1 < array.length) {
                        recordType += array[i];
                    }

                }
                if (offset + 1 < array.length) {
                    offset += 1;
                }

                // extraction of unixtimestamp
                String unixTimestamp = "";
                for (int j = offset; j < offset + 4; j++) {
                    if (offset + 4 < array.length) {
                        unixTimestamp += array[j];
                    }
                }
                if (offset + 4 < array.length) {
                    offset += 4;
                }

                //extraction of userId
                String userId = "";
                for (int k = offset; k < offset + 8; k++) {
                    if (offset + 8 < array.length) {
                        userId += array[k];
                    }

                }

                byte[] userBytes = userId.getBytes();
                if (offset + 8 < array.length) {
                    offset += 8;
                }

                // conversion of recordType
                String recordTypeText = bytesToHex(recordType.getBytes());

                //conversion of unixtimestamp
                byte[] unixTimestampBytes = unixTimestamp.getBytes();
                int tempoffset = 2;

                if (recordTypeText.equals("00") || recordTypeText.equals("01")) {

                    // extraction of amount and conversion
                    double dollarAmt = 0;
                    String dollarAmtText = "";
                    for (int i = offset; i < offset + 8; i++) {
                        if (offset + 8 < array.length) {
                            dollarAmtText += array[i];
                        }

                    }

                    byte[] dollarByte = dollarAmtText.getBytes();
                    double dollarAmount = ByteBuffer.wrap(dollarByte).getDouble();


                    // calculating debit and credit
                    if (recordTypeText.equals("00")) {

                        creditAmount += dollarAmount;
                    } else {
                        debitAmount += dollarAmount;
                    }

                    // calculating balance for user 2456938384156277127
                    if (new BigInteger(userBytes) == new BigInteger("2456938384156277127".getBytes())) {
                        balanceForUser2456938384156277127 += dollarAmount;
                    }

                    // data to be printed here.
                    System.out.println("" + recordTypeText + " | " + new Date(byteArrayToUInt32(unixTimestampBytes, loffset) * 1000).toLocaleString() + " | " + new BigInteger(userBytes) + " | " + df.format(dollarAmount));

                    if (offset + 8 < array.length) {
                        offset += 8;
                    }
                } else {

                    // calculating start autopay and end autopay
                    if (recordTypeText.equals("02")) {

                        startAutopay += 1;
                    } else if (recordTypeText.equals("03")) {
                        endAutopay += 1;
                    } else {
                    }

                    // data is printed here
                    System.out.println("" + recordTypeText + " | " + new Date(byteArrayToUInt32(unixTimestampBytes, loffset) * 1000).toLocaleString() + " | " + new BigInteger(userBytes));

                }


            }



            // fin.close();
        } catch (Exception e) {
          //  System.out.println(e);
        }

        // final message is printed here after calculation.
        System.out.println("total credit amount= " + df.format(creditAmount));
        System.out.println("total debit amount= " + df.format(debitAmount));
        System.out.println("autopays started = " + startAutopay);
        System.out.println("autopays ended = " + endAutopay);
        System.out.println("balance for user 2456938384156277127 = " + balanceForUser2456938384156277127);
    }

    public static long byteArrayToUInt32(byte[] data, int offset) {
        if (data == null || data.length < offset + 4) {
            return 0;
        }
        long result = ((data[offset] & 0xFF) << 24)
                + ((data[offset + 1] & 0xFF) << 16)
                + ((data[offset + 2] & 0xFF) << 8)
                + (data[offset + 3] & 0xFF);

        return result;
    }

    private static final byte[] HEX_ARRAY = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII);
    public static String bytesToHex(byte[] bytes) {
        byte[] hexChars = new byte[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars, StandardCharsets.UTF_8);
    }


}

