package io.delta.kernel.internal.compaction;

import static io.delta.kernel.internal.DeltaErrors.wrapEngineExceptionThrowsIO;
import static io.delta.kernel.internal.util.Utils.toCloseableIterator;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.delta.kernel.utils.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Writer for writing log compaction files */
public class LogCompactionWriter {

  private static final Logger logger = LoggerFactory.getLogger(LogCompactionWriter.class);

  private final Path logPath;
  private final long startVersion;
  private final long endVersion;

  public LogCompactionWriter(Path logPath, long startVersion, long endVersion) {
    this.logPath = requireNonNull(logPath);
    this.startVersion = startVersion;
    this.endVersion = endVersion;
  }

  /**
   * Writes a compacted JSON file that merges all commit JSON lines from startVersion to endVersion.
   */
  public void writeMinorCompactionLog(Engine engine) throws IOException {
    String fileName = String.format("%020d.%020d.compacted.json", startVersion, endVersion);
    Path compactedPath = new Path(logPath, fileName);

    logger.info("Writing compacted log file to path: {}", compactedPath);

    // TODO: Would we be better off with some helpers in log replay here?
    List<FileStatus> commitFiles = new ArrayList<>();
    for (long v = startVersion; v <= endVersion; v++) {
      String commitPath = FileNames.deltaFile(logPath, v);
      commitFiles.add(FileStatus.of(commitPath.toString(), 0, 0));
    }

    try (CloseableIterator<ColumnarBatch> batches =
        engine
            .getJsonHandler()
            .readJsonFiles(
                toCloseableIterator(commitFiles.iterator()),
                AddFile.FULL_SCHEMA,
                Optional.empty())) {

      wrapEngineExceptionThrowsIO(
          () -> {
            try (CloseableIterator<Row> rowIterator = new ColumnarBatchListToRowIterator(batches)) {

              // TODO: Add any additional transformation.
              engine
                  .getJsonHandler()
                  .writeJsonFileAtomically(compactedPath.toString(), rowIterator, false);
            }

            logger.info("Successfully wrote compacted log file `{}`", compactedPath);
            return null;
          },
          "Write compacted log file `%s`",
          compactedPath);
    }
  }

  /**
   * Utility to determine if log compaction should run for the given commit version.
   */
  public static boolean shouldCompact(long commitVersion, long compactionInterval) {
    return commitVersion > 0 && (commitVersion % compactionInterval == 0);
  }

  /**
   * Iterator to convert FilteredColumnarBatch to a stream of Row objects.
   * TODO:? This could be a separate utility class?
   */
  private static class ColumnarBatchListToRowIterator implements CloseableIterator<Row> {
    private final CloseableIterator<ColumnarBatch> addFiles;
    private CloseableIterator<Row> currentBatchRows;
    private boolean isClosed = false;

    public ColumnarBatchListToRowIterator(CloseableIterator<ColumnarBatch> addFiles) {
      this.addFiles = addFiles;
    }

    @Override
    public boolean hasNext() {
      if (isClosed) {
        return false;
      }

      while ((currentBatchRows == null || !currentBatchRows.hasNext()) && addFiles.hasNext()) {
        if (currentBatchRows != null) {
          try {
            currentBatchRows.close();
          } catch (IOException e) {
            logger.warn("Error closing previous batch rows", e);
          }
        }

        ColumnarBatch nextBatch = addFiles.next();
        currentBatchRows = nextBatch.getRows();
      }

      return currentBatchRows != null && currentBatchRows.hasNext();
    }

    @Override
    public Row next() {
      if (!hasNext()) {
        throw new java.util.NoSuchElementException("No more rows available");
      }
      return currentBatchRows.next();
    }

    @Override
    public void close() throws IOException {
      isClosed = true;

      if (currentBatchRows != null) {
        currentBatchRows.close();
      }

      addFiles.close();
    }
  }
}