package com.qi;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

public class HDFSUtil01 {
	static Configuration conf = new Configuration();
	static FileSystem fs;

	/* @Before */
	public static void init() {
		conf.set("fs.defaultFS", "hdfs://master:9000");
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 上传
	 */
	@Test
	public void uploadFileToHDFS() {
		try {
			fs.copyFromLocalFile(false, new Path("E:\\upload\\linux命令记录.txt"), new Path("/20170902/data/linux命令记录"));
			fs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * 删除
	 */
	@Test
	public void deleteHDFSFile() {

		String src = "/aaa";
		Path path = new Path(src);
		try {
			fs.delete(path, true);
			fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * 创建文件夹
	 */
	@Test
	public void createDirOnHDFS() {

		String src = "/aaa";
		Path path = new Path(src);
		try {
			fs.mkdirs(path);
			fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * 创建文件并添加内容
	 */
	@Test
	public void createFileAddContent() {
		String content = "我是李四";
		String src = "/ccc.txt";
		try (FSDataOutputStream fos = fs.create(new Path(src), true);) {
			fos.write(content.getBytes());
			fs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 复制文件
	 */
	@Test
	public void copyFileInHDFS() {

		try (FSDataInputStream fis = fs.open(new Path("/ccc.txt"));
				FSDataOutputStream fos = fs.create(new Path("/ddd.txt"))) {
			byte[] buffer = new byte[1024];
			while (fis.read(buffer) != -1) {
				fos.write(buffer);
			}
			fs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 下载文件
	 */
	@Test
	public void downloadFileFromHDFS() {

		try {
			fs.copyToLocalFile(new Path("E:\\upload\\"), new Path("/install.log"));
			fs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * 重命名文件
	 */
	@Test
	public void renameFileFromHDFS() {

		try {
			fs.rename(new Path("/ddd.txt"), new Path("/eee.txt"));
			fs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Test
	public void getFileFromHDFSDir() {

		try {
			RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
			while (listFiles.hasNext()) {
				LocatedFileStatus next = listFiles.next();
				Path path = next.getPath();
				System.out.println(path);
			}
			fs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	//文件夹或文件距离左侧的空格数,一级目录或文件距离左侧一个空格
	static int i = 1;
	public static void listStatus(FileSystem fs, Path path) throws FileNotFoundException, IOException {
		FileStatus[] listStatus = fs.listStatus(path);
		for (FileStatus fileStatu : listStatus) {
			//获得文件夹或文件的路径
			Path path2 = fileStatu.getPath();
			//判断此项为文件夹或文件
			boolean isDirectory = fileStatu.isDirectory();
			//输出文件夹或文件距离左侧的空格
			System.out.printf("%" + i + "s", " ");
			//输出文件夹或文件的名称
			System.out.println((isDirectory ? "*" : "-") + path2);
			if (isDirectory) {
				//如果此项为文件夹则此文件夹中的文件夹或文件空格数+2
				i += 2;
				//遍历此项文件夹中的内容
				listStatus(fs, path2);
				//文件夹中没有文件夹且遍历完文件后,接着遍历与该文件夹同级的文件夹或文件,且空格数-2
				i-=2;
			}
		}

	}

	public static void main(String[] args) throws FileNotFoundException, IllegalArgumentException, IOException {
		init();
		listStatus(fs, new Path("/"));
	}

}
