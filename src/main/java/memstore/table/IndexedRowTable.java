package memstore.table;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * IndexedRowTable, which stores data in row-major format.
 * That is, data is laid out like
 *   row 1 | row 2 | ... | row n.
 *
 * Also has a tree index on column `indexColumn`, which points
 * to all row indices with the given value.
 */
public class IndexedRowTable implements Table {

    int numCols;
    int numRows;
    private TreeMap<Integer, IntArrayList> index;
    private ByteBuffer rows;
    private int indexColumn;

    public IndexedRowTable(int indexColumn) {
        this.indexColumn = indexColumn;
    }

    /**
     * Loads data into the table through passed-in data loader. Is not timed.
     *
     * @param loader Loader to load data from.
     * @throws IOException
     */
    @Override
    public void load(DataLoader loader) throws IOException {
        // TODO: Implement this!
        this.numCols = loader.getNumCols();
        List<ByteBuffer> rows = loader.getRows();
        this.numRows = rows.size();
        this.rows = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);
        this.index = new TreeMap<Integer, IntArrayList>();

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            for (int colId = 0; colId < numCols; colId++) {
                int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
                this.rows.putInt(offset, curRow.getInt(ByteFormat.FIELD_LEN * colId));

                // update index data structure
                if (colId == this.indexColumn) {
                    int key = curRow.getInt(ByteFormat.FIELD_LEN * colId);
                    if (! this.index.containsKey(key)) {
                        this.index.put(key, new IntArrayList());
                    }
                    this.index.get(key).add(rowId);
                }
            }
        }
    }

    /**
     * Returns the int field at row `rowId` and column `colId`.
     */
    @Override
    public int getIntField(int rowId, int colId) {
        // TODO: Implement this!
        int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
        return this.rows.getInt(offset);
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {
        // TODO: Implement this!
        int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
        this.rows.putInt(offset, field);
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table;
     *
     *  Returns the sum of all elements in the first column of the table.
     */
    @Override
    public long columnSum() {
        // TODO: Implement this!
        int sum = 0;
        for (int rowId = 0; rowId < this.numRows; rowId++) {
            sum += this.getIntField(rowId, 0);
        }
        return sum;
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table WHERE col1 > threshold1 AND col2 < threshold2;
     *
     *  Returns the sum of all elements in the first column of the table,
     *  subject to the passed-in predicates.
     */
    @Override
    public long predicatedColumnSum(int threshold1, int threshold2) {
        // TODO: Implement this!
        int sum = 0;
        for (int rowId = 0; rowId < this.numRows; rowId++) {
            if (this.getIntField(rowId, 1) > threshold1 && this.getIntField(rowId, 2) < threshold2) {
                sum += this.getIntField(rowId, 0);
            }
        }
        return sum;
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) + SUM(col1) + ... + SUM(coln) FROM table WHERE col0 > threshold;
     *
     *  Returns the sum of all elements in the rows which pass the predicate.
     */
    @Override
    public long predicatedAllColumnsSum(int threshold) {
        // use index to retrieve the map of all rows where col0 > threshold
        NavigableMap<Integer, IntArrayList> subMap = this.index.tailMap(threshold, false);

        int sum = 0;
        for (Integer key: subMap.keySet()) {
            // loop through all the rows that contain this key
            for (int rowId : subMap.get(key)) {
                // loop through all the columns for this row for summation
                for (int colId = 0; colId < this.numCols; colId++) {
                    sum += this.getIntField(rowId, colId);
                }
            }
        }
        return sum;
    }

    /**
     * Implements the query
     *   UPDATE(col3 = col3 + col2) WHERE col0 < threshold;
     *
     *   Returns the number of rows updated.
     */
    @Override
    public int predicatedUpdate(int threshold) {
        // TODO: Implement this!
        // use index to retrieve the map of all rows where col0 < threshold
        NavigableMap<Integer, IntArrayList> subMap = this.index.headMap(threshold, false);

        int rowsUpdated = 0;
        for (Integer key: subMap.keySet()) {
            // loop through all the rows that contain this key
            for (int rowId : subMap.get(key)) {
                // update the entry of col 3 for this row
                int newValue = this.getIntField(rowId, 3) + this.getIntField(rowId, 2);
                this.putIntField(rowId, 3, newValue);
                rowsUpdated += 1;
            }
        }
        return rowsUpdated;
    }
}
