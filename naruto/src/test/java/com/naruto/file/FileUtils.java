package com.naruto.file;

import org.apache.commons.lang3.StringUtils;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @author gin
 * @date 2019/12/3 10:23
 */
public class FileUtils {

    public static final String DATA_SPLIT = "\u0003";

    public static final String PARENT_PATH = "..";
    public static final String CURRENT_PATH = ".";

    public static final String LINUX_FILE_SEPARATOR = "/";


    public static boolean isNormalDir(File childFile) {
        return childFile.isDirectory() && !CURRENT_PATH.equals(childFile.getName()) && !PARENT_PATH.equals(childFile.getName());
    }


    public static void main(String[] args) {
        //splitDataReturnFile(1000, new File("D:/var/big.txt"), "D:/var/temp");

        List<String> lineToList = readLineToList(new File("D:/var/ODS.CCD_TMP_STMA_1208_0X03_TXT.txt"));
        System.out.println(lineToList);
        List<Map<String, Object>> maps = splitData("1\u00032\u00033\u00034\u00035\u00036\u00037\u00038\u00039\u000310\u000311\u000312\u000313\u000314\u000315\u000316\u000317\u000318\u000319\u000320\u000321\u000322\u000323\u000324\u000325\u000326\u000327\u000328\u000329\u000330", lineToList);
        System.out.println(maps);
    }

    public static List<File> splitDataReturnFile(int rows, File sourceFile, String targetDirectoryPath) {
        long startTime = System.currentTimeMillis();
        List<File> fileList = new ArrayList<>();
        System.out.println("begin splitDataReturnFile");
        File targetFile = new File(targetDirectoryPath);
        if (!sourceFile.exists() || rows <= 0 || sourceFile.isDirectory()) {
            return null;
        }
        if (targetFile.exists()) {
            if (!targetFile.isDirectory()) {
                return null;
            }
        } else {
            targetFile.mkdirs();
        }

        try (FileInputStream fileInputStream = new FileInputStream(sourceFile);
             InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, StandardCharsets.UTF_8);
             BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
            //把每次读取到的行缓存起来,不要每次都写道文件里面(效率低),累计到指定行数再一次写入
            StringBuilder stringBuilder = new StringBuilder();
            String lineStr;
            int lineNo = 1, fileNum = 1;
            while ((lineStr = bufferedReader.readLine()) != null) {
                stringBuilder.append(lineStr).append("\r\n");
                if (lineNo % rows == 0) {
                    //File file = new File(targetDirectoryPath + FileUtils.LINUX_FILE_SEPARATOR + fileNum + sourceFile.getName());
                    File file = new File(targetDirectoryPath + FileUtils.LINUX_FILE_SEPARATOR + fileNum);
                    writeFile(stringBuilder.toString(), file);
                    //清空文本
                    stringBuilder.delete(0, stringBuilder.length());
                    fileNum++;
                    fileList.add(file);
                }
                lineNo++;
            }
            if ((lineNo - 1) % rows != 0) {
                File file = new File(targetDirectoryPath + FileUtils.LINUX_FILE_SEPARATOR + fileNum + sourceFile.getName());
                writeFile(stringBuilder.toString(), file);
                fileList.add(file);
            }
            long endTime = System.currentTimeMillis();
            System.out.println("splitDataReturnFile end,cost:" + ((endTime - startTime) / 1000) + "second");
        } catch (Exception e) {
            System.out.println("splitDataReturnFile error:" + e);
        }
        return fileList;
    }

    public static void splitDataToFile(int rows, File sourceFile, String targetDirectoryPath) {
        long startTime = System.currentTimeMillis();
        System.out.println("begin splitDataToFile");
        File targetFile = new File(targetDirectoryPath);
        if (!sourceFile.exists() || rows <= 0 || sourceFile.isDirectory()) {
            return;
        }
        if (targetFile.exists()) {
            if (!targetFile.isDirectory()) {
                return;
            }
        } else {
            targetFile.mkdirs();
        }

        try (FileInputStream fileInputStream = new FileInputStream(sourceFile);
             InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, StandardCharsets.UTF_8);
             BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
            //把每次读取到的行缓存起来,不要每次都写道文件里面(效率低),累计到指定行数再一次写入
            StringBuilder stringBuilder = new StringBuilder();
            String lineStr;
            int lineNo = 1, fileNum = 1;
            while ((lineStr = bufferedReader.readLine()) != null) {
                stringBuilder.append(lineStr).append("\r\n");
                if (lineNo % rows == 0) {
                    File file = new File(targetDirectoryPath + FileUtils.LINUX_FILE_SEPARATOR + fileNum);
                    writeFile(stringBuilder.toString(), file);
                    //清空文本
                    stringBuilder.delete(0, stringBuilder.length());
                    fileNum++;
                }
                lineNo++;
            }
            if ((lineNo - 1) % rows != 0) {
                File file = new File(targetDirectoryPath + FileUtils.LINUX_FILE_SEPARATOR + fileNum + sourceFile.getName());
                writeFile(stringBuilder.toString(), file);
            }
            long endTime = System.currentTimeMillis();
            System.out.println("splitDataToFile end,cost:" + ((endTime - startTime) / 1000) + "second");
        } catch (Exception e) {
            System.out.println("splitDataToFile error:" + e);
        }
    }

    private static void writeFile(String text, File file) {
        try (
                FileOutputStream fileOutputStream = new FileOutputStream(file);
                OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8);
                BufferedWriter bufferedWriter = new BufferedWriter(outputStreamWriter, 1024)
        ) {
            bufferedWriter.write(text);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 获取文本文件的总行数
     *
     * @param filePath
     */
    public static int getCountLine(String filePath) {
        //总行号
        int lineNumber = 1;
        try {
            //使用这个类,可以直接跳过字符,快速获取行号
            LineNumberReader reader = new LineNumberReader(new FileReader(filePath));
            //获取文件的总大小
            File file = new File(filePath);
            long count = file.length();
            //跳过字符,获取到当前行号,就是总行号
            reader.skip(count);
            lineNumber = reader.getLineNumber();
        } catch (Exception e) {
            System.out.println("getCountLine error:" + filePath + e.getMessage());
        }
        return lineNumber;
    }

    public static List<String> readLineToList(File file) {
        List<String> dataList = new LinkedList<>();
        InputStreamReader read = null;
        try {
            read = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8);
            BufferedReader bufferedReader = new BufferedReader(read);
            String lineTxt = null;
            while ((lineTxt = bufferedReader.readLine()) != null) {
                dataList.add(lineTxt);
            }
            read.close();
        } catch (Exception e) {
            System.out.println("file parse String error:" + e);
        } finally {
            try {
                if (null != read) {
                    read.close();
                }
            } catch (IOException e) {
                System.out.println("io close error:" + e);
            }
        }
        return dataList;
    }

    public static List<Map<String, Object>> splitData(String fieldName, List<String> fileResult) {
        LinkedList<Map<String, Object>> dataMapList = new LinkedList<>();

        try {
            //获取字段数组
            String[] filedArray = fieldName.split(DATA_SPLIT);
            String[] splitList;
            HashMap<String, Object> dataMap;
            for (String data : fileResult) {
                if (StringUtils.isNotBlank(data) && data.contains(DATA_SPLIT)) {
                    //将每行客户信息进行分割
                    splitList = data.split(DATA_SPLIT);
                    dataMap = new HashMap<>();
                    for (int i = 0; i < splitList.length; i++) {
                        if (StringUtils.isNotBlank(splitList[i])) {
                            //与字段数组匹配成map,要求文件中每行数据顺序和字段数组(配置文件中)顺序一致
                            dataMap.put(filedArray[i], splitList[i]);
                        }
                    }
                    dataMapList.add(dataMap);
                }
            }
        } catch (Exception e) {
            System.out.println("splitData error: \n" + e);
        }
        return dataMapList;
    }

    /**
     * 文件操作区域
     * <p>
     * 验证字符串是否为正确路径名的正则表达式
     */
    private static String matches = "[A-Za-z]:\\\\[^:?\"><*]*";

    // 根据路径删除指定的目录或文件，无论存在与否
    public static boolean DeleteFolder(String deletePath) {
        // 通过路径字符串 deletePath.matches(matches) 方法的返回值判断是否正确
        if (deletePath.matches(matches)) {
            File file = new File(deletePath);
            if (!file.exists()) {
                // 目录或文件不存在返回 false
                return false;
            } else {
                if (file.isFile()) {// 判断是否为文件
                    return deleteFile(deletePath);// 为文件时调用删除文件方法
                } else {
                    return deleteDirectory(deletePath);// 为目录时调用删除目录方法
                }
            }
        } else {
            System.out.println("please input corrected path");
            return false;
        }
    }

    /**
     * 删除单个文件(非目录)
     *
     * @param filePath 文件路径
     * @return
     */
    public static boolean deleteFile(String filePath) {
        File file = new File(filePath);
        // 路径为文件且不为空则进行删除
        if (file.isFile() && file.exists()) {
            // 文件删除
            return file.delete();
        }
        return false;
    }

    /**
     * 删除目录（文件夹）以及目录下的文件
     *
     * @param dirPath 需要被删目录
     * @return 是否成功
     */
    public static boolean deleteDirectory(String dirPath) {
        // 如果path不以文件分隔符结尾，自动添加文件分隔符
        if (!dirPath.endsWith(FileUtils.LINUX_FILE_SEPARATOR)) {
            dirPath = dirPath + FileUtils.LINUX_FILE_SEPARATOR;
        }
        File dirFile = new File(dirPath);
        // 如果dir对应的文件不存在，或者不是一个目录，则退出
        if (!dirFile.exists() || !dirFile.isDirectory()) {
            return false;
        }
        boolean flag = true;
        // 获得传入路径下的所有文件
        File[] files = dirFile.listFiles();
        if (null != files && files.length > 0) {
            // 循环遍历删除文件夹下的所有文件(包括子目录)
            for (File file : files) {
                if (file.isFile() && file.exists()) {
                    // 删除子文件
                    flag = deleteFile(file.getAbsolutePath());
                    System.out.println(file.getAbsolutePath() + " ---- successfully delete ----");
                    if (!flag) {
                        // 如果删除失败，则跳出
                        break;
                    }
                } else if (isNormalDir(file)) {
                    // 运用递归，删除子目录
                    flag = deleteDirectory(file.getAbsolutePath());
                    System.out.println(file.getAbsolutePath() + " ---- successfully delete ----");
                    if (!flag) {
                        System.out.println("error dir name=" + file.getName());
                        // 如果删除失败，则跳出
                        break;
                    }
                } else {
                    System.out.println("unknown file name=" + file.getName());
                }
            }
        }
        if (!flag) {
            return false;
        }
        // 删除当前目录
        return dirFile.delete();
    }

    // 创建目录
    public static boolean createDir(String destDirName) {
        File dir = new File(destDirName);
        if (dir.exists()) {// 判断目录是否存在
            System.out.println("createDir error: already exists");
            return false;
        }
        if (!destDirName.endsWith(FileUtils.LINUX_FILE_SEPARATOR)) {
            // 结尾是否以"/"结束
            destDirName = destDirName + FileUtils.LINUX_FILE_SEPARATOR;
        }
        if (dir.mkdirs()) {// 创建目标目录
            System.out.println("successfully create dir: " + destDirName);
            return true;
        } else {
            System.out.println("failed create dir: " + destDirName);
            return false;
        }
    }

}
