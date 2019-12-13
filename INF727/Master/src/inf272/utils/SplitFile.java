package inf272.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.StringWriter;
import java.nio.charset.CharsetDecoder;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class SplitFile {

	/**
	 * Split the file in small part. Choose the minimal ckunk size (64Mo by default)
	 * and minimal split part (1 by default
	 */

	private int splits = 1;
	//private int chunk = 64 * 1024 * 1024;

	public SplitFile() {
	}

	public SplitFile(int splits) {
		this.splits = splits;
	}

	public SplitFile(int splits, int chunk) {
		this.splits = splits;
	//	this.chunk = chunk;
	}

	/**
	 * 
	 * @param f
	 * @throws IOException
	 */
	public List<File> splitFile(File f) throws IOException {
		ArrayList<File> list = new ArrayList<>();

		if (f.exists()) {

			// Let's chunk!
			long fileSize = Files.size(f.toPath());
			long chunks = splits;//fileSize / chunk;

			// Si on a un fichier petit mais qu'on veut decouper en petit bout, on le permet
//			if (chunks < splits)
//				chunks = splits;

			long offset = fileSize / chunks;
			long end = fileSize;
			RandomAccessFile raf = new RandomAccessFile(f.getAbsolutePath(), "r");

			// Read the first line to
			String str = raf.readLine();
			int ss = str.getBytes().length / str.length();
			// Find the closest '\n' starting from the end

			int n = 0;

			for (n = (int) chunks; n > 0; n--) {

				long start = end - offset;
				boolean findStart = false;
				
				if(n==1)
					start = 0;
				
				if (start <= 0) {
					start = 0;
					findStart = true;

				}
				// Starting with a even byte to find a char
				if (ss == 2 && start % 2 != 0)
					start++;

				raf.seek(start);
				long off = 0;

				while (!findStart && start + off * ss < end) {

					long s = start + off * ss;
					raf.seek(s);

					if (ss == 2) {
						char c = raf.readChar();
						if (c == '\n')
							findStart = true;
					} else {
						byte b = raf.readByte();
						if (b == '\n')
							findStart = true;
					}
					if (!findStart)
						off++;
				}

				if (start + (off + 1) * ss >= end) {
					off = 0;
					findStart = false;
					while (!findStart && start + off * ss >= 0) {

						long s = start + off * ss;
						raf.seek(s);

						if (ss == 2) {
							char c = raf.readChar();
							if (c == '\n')
								findStart = true;
						} else {
							byte b = raf.readByte();
							if (b == '\n')
								findStart = true;
						}
						off--;
					}

				}

				// Copy by part of 1Mo each time
				byte[] buffer = new byte[(int) (end - start - off * ss)];

				int readed = raf.read(buffer);
				RandomAccessFile target = new RandomAccessFile("S"+ n + ".txt", "rw");
				list.add(new File("S"+n+".txt"));
				
				
				target.write(buffer,0,readed);
				target.close();
				end = start + off * ss;
			}

		}

		return list;
	}
}
