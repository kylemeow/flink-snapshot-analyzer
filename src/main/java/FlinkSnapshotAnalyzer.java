import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;

import static org.apache.flink.runtime.checkpoint.Checkpoints.loadCheckpointMetadata;

/**
 * Utility for reading & displaying statistics about Flink Checkpoint / Savepoint
 */
@Slf4j
public class FlinkSnapshotAnalyzer {
	public static final int ILLEGAL_INPUT_ARGUMENT = -1;
	public static final int ILLEGAL_METADATA_FILEPATH = -2;

	public static void main(String[] args) throws IOException {
		if (args.length == 0) {
			log.error("Please input a Flink snapshot path as the first argument");
			System.exit(ILLEGAL_INPUT_ARGUMENT);
		}

		String savepointDirectory = args[0];
		log.info("User has provided snapshot path {}", savepointDirectory);

		File metaFile = new File(savepointDirectory + File.separator + "_metadata");
		if (!metaFile.exists() || !metaFile.isFile()) {
			log.error("Metafile {} is not a file or does not exist", metaFile.getPath());
			System.exit(ILLEGAL_METADATA_FILEPATH);
		}

		byte[] bytes = FileUtils.readFileToByteArray(metaFile);
		DataInputStream in = new DataInputViewStreamWrapper(new ByteArrayInputStreamWithPos(bytes));
		CheckpointMetadata deserializedMetadata = loadCheckpointMetadata(in, FlinkSnapshotAnalyzer.class.getClassLoader(), metaFile.getAbsolutePath());

		log.info("\n\n=========== Summary ===========");
		log.info("Checkpoint ID: {}", deserializedMetadata.getCheckpointId());
		log.info("Master States: {}", deserializedMetadata.getMasterStates());
		log.info("Operator States: {}", deserializedMetadata.getOperatorStates());

		PriorityQueue<Tuple2<OperatorState, Long>> stateSizeQueue = new PriorityQueue<>(new OperatorStateSizeComparator());

		double totalStateSizes = 0;
		for (OperatorState operatorState : deserializedMetadata.getOperatorStates()) {
			long stateSizeForOperator = 0;
			for (Map.Entry<Integer, OperatorSubtaskState> entry : operatorState.getSubtaskStates().entrySet()) {
				totalStateSizes += entry.getValue().getStateSize();
				stateSizeForOperator += entry.getValue().getStateSize();
			}
			stateSizeQueue.add(new Tuple2<>(operatorState, stateSizeForOperator));
		}

		while (!stateSizeQueue.isEmpty()) {
			Tuple2<OperatorState, Long> operatorStateTuple = stateSizeQueue.poll();
			OperatorState operatorState = operatorStateTuple.f0;
			log.info("\n\n-> [{}%] {}MB Operator ID {}",
					operatorStateTuple.f1 / totalStateSizes * 100,
					operatorStateTuple.f1 / 1024 / 1024,
					operatorState.getOperatorID());

			for (Map.Entry<Integer, OperatorSubtaskState> entry : operatorState.getSubtaskStates().entrySet()) {
				log.info("[{} MB] {}: Subtask States: {}",
						entry.getValue().getStateSize() / 1024 / 1024,
						entry.getKey(),
						entry.getValue());
			}
		}
	}

	public static class OperatorStateSizeComparator implements Comparator<Tuple2<OperatorState, Long>> {
		@Override
		public int compare(Tuple2<OperatorState, Long> o1, Tuple2<OperatorState, Long> o2) {
			return Long.compare(o1.f1, o2.f1);
		}
	}
}
