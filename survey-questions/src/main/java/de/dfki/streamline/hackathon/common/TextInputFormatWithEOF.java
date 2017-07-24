package de.dfki.streamline.hackathon.common;


import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Either;

import java.io.IOException;

public class TextInputFormatWithEOF extends DelimitedInputFormat<Either<String, EOFMarker>> {


	private static final long serialVersionUID = 1L;

	/**
	 * Code of \r, used to remove \r from a line when the line ends with \r\n
	 */
	private static final byte CARRIAGE_RETURN = (byte) '\r';

	/**
	 * Code of \n, used to identify if \n is used as delimiter
	 */
	private static final byte NEW_LINE = (byte) '\n';

	/**
	 * The name of the charset to use for decoding.
	 */
	private String charsetName = "UTF-8";

	public TextInputFormatWithEOF(Path filePath, FilePathFilter defaultFilter) {
		super(filePath, null);
		setFilesFilter(defaultFilter);
	}

	@Override
	public Either<String, EOFMarker> readRecord(Either<String, EOFMarker> reuse, byte[] bytes, int offset, int numBytes) throws IOException {

		//Check if \n is used as delimiter and the end of this line is a \r, then remove \r from the line
		if (this.getDelimiter() != null && this.getDelimiter().length == 1
				&& this.getDelimiter()[0] == NEW_LINE && offset+numBytes >= 1
				&& bytes[offset+numBytes-1] == CARRIAGE_RETURN){
			numBytes -= 1;
		}

		return Either.Left(new String(bytes, offset, numBytes, this.charsetName));
	}


	@Override
	public Either<String, EOFMarker> nextRecord(Either<String, EOFMarker> record) throws IOException {
		Either<String, EOFMarker> ret = super.nextRecord(record);
		if (ret == null || ret.left() == null) {
			return Either.Right(new EOFMarker());
		}
		return ret;
	}

}
