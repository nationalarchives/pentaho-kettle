/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.core.row;

import java.util.List;

/**
 * This class of static methods can be used to manipulate rows: add, delete, resize, etc... That way, when we want to go
 * for a metadata driven system with hiding deletes, over sized arrays etc, we can change these methods to find
 * occurrences.
 * <p>
 * For example, a step adding a field to the row should always call
 *
 * <pre>
 * <a href="">public static Object[] resizeArray(Object[] objects, int newSize)</a></i>
 * </pre>
 *
 * which will either physically resize the array or return the original row, in case it was over-allocated and has
 * enough slots. If a step needs to create new rows from scratch, it should use allocateRowData() which will return a
 * somewhat over-allocated object array to fit the desired number of fields.
 *
 * @author Matt
 */
public class RowDataUtil {
  public static int OVER_ALLOCATE_SIZE = 10;

  /**
   * Allocate a new Object array. However, over allocate by a constant factor to make adding values faster.
   *
   * @param size
   *          the minimum size to allocate.
   * @return the newly allocated object array
   */
  public static Object[] allocateRowData( int size ) {
    return new Object[size + OVER_ALLOCATE_SIZE];
  }

  /**
   * Resize an object array making it bigger, over allocate, return the original array if there's enough room.
   *
   * @param objects
   * @param newSize
   * @return A new object array, resized.
   */
  public static Object[] resizeArray( Object[] objects, int newSize ) {

    if ( objects != null && objects.length >= newSize ) {
      return objects;
    }

    Object[] newObjects = new Object[newSize + OVER_ALLOCATE_SIZE];
    if ( objects != null ) {
      System.arraycopy( objects, 0, newObjects, 0, objects.length );
    }
    return newObjects;
  }

  /**
   * Resize an object array making it bigger, over allocate, always create a copy of the original array, even if there's
   * enough room in the old one.
   *
   * @param objects
   *          the original row
   * @param newSize
   *          the new size
   * @return A new object array, resized.
   */
  public static Object[] createResizedCopy( Object[] objects, int newSize ) {

    Object[] newObjects;
    if ( objects.length < newSize ) {
      newObjects = new Object[newSize + OVER_ALLOCATE_SIZE];
    } else {
      newObjects = new Object[objects.length];
    }

    if ( objects != null ) {
      System.arraycopy( objects, 0, newObjects, 0, objects.length );
    }
    return newObjects;
  }

  /**
   * This method concatenates data from an array of rows, each with their own specific length.
   *
   * @param objects
   * @param lengths
   * @return The concatenated array of objects.
   */
  public static Object[] createResizedCopy( Object[][] objects, int[] lengths ) {
    int size = 0;
    if ( objects != null ) {
      for ( int i = 0; i < objects.length; i++ ) {
        size += lengths[i];
      }
    }
    Object[] newObjects = allocateRowData( size );

    if ( objects != null ) {
      size = 0;
      for ( int i = 0; i < lengths.length; i++ ) {
        System.arraycopy( objects[i], 0, newObjects, size, lengths[i] );
        size += lengths[i];
      }
    }

    return newObjects;
  }

  public enum InputRowSelectionPolicy {
    /**
     * Use the field from the first matching input row.
     */
    USE_FIRST,

    /**
     * Use the field from the first matching input row, unless
     * the value of the field is null, then try the next input row
     * until we match one where the field value is non-null. NOTE: If all
     * input rows that have matching field values are null, then
     * ultimately the field value must be null.
     */
    USE_FIRST_NOT_NULL,

    /**
     * Use the field from the last matching input row.
     */
    USE_LAST,

    /**
     * Use the field from the last matching input row, unless
     * the value of the field is null, then try the previous input row
     * until we match one where the field value is non-null. NOTE: If all
     * input rows that have matching field values are null, then
     * ultimately the field value must be null.
     */
    USE_LAST_NOT_NULL;

    /**
     * Does this policy iterate forward through the input rows?
     *
     * @return true if the policy should iterate forward, false if it iterates backward.
     */
    public boolean iterateForward() {
      return this == USE_FIRST || this == USE_FIRST_NOT_NULL;
    }

    /**
     * Does this policy avoid null values?
     *
     * @return true if null values should be avoided.
     */
    public boolean avoidNull() {
      return this == USE_FIRST_NOT_NULL || this == USE_LAST_NOT_NULL;
    }
  }

  /**
   * Copies only the fields from the input rows that are required in the output row.
   *
   * If there is more than one input row with a field of the same name, that is required
   * in the output row, then input row is selected according to the {@link InputRowSelectionPolicy}.
   *
   * @param inputRows the input rows.
   * @param inputRowMetas the metas for each input row.
   * @param inputRowSelectionPolicy the policy to use in selecting an input row when there is more than one
   *                                input row containing the same named field that is required in the output row.
   * @param outputRowMetaList the details of the fields required in the output row.
   *
   * @return the output row.
   */
  public static Object[] createCustomResizedCopy(final Object[][] inputRows, final RowMetaInterface[] inputRowMetas,
      final InputRowSelectionPolicy inputRowSelectionPolicy, final List<ValueMetaInterface> outputRowMetaList) {

    final Object[] outputRow = allocateRowData(outputRowMetaList.size());

    // iterate over all the required output row fields
    for (int idxOutputRowField = 0; idxOutputRowField < outputRowMetaList.size(); idxOutputRowField++) {
      final ValueMetaInterface outputFieldMeta = outputRowMetaList.get(idxOutputRowField);
      final String outputFieldName = outputFieldMeta.getName();

      Object inputRowFieldValue = null;
      int idxInputRowField = -1;

      // dependent on the InputRowSelectionPolicy set parameters for the below loop
      // so that the loop iterates either forward or backward through the input rows
      int idxInputRow;
      final int step;
      final int end;
      if (inputRowSelectionPolicy.iterateForward()) {
        idxInputRow = 0;
        step = 1;
        end = inputRowMetas.length;
      } else {
        idxInputRow = inputRowMetas.length - 1;
        step = -1;
        end = -1;
      }

      // find a corresponding field in one of the input rows
      for (; idxInputRow != end; idxInputRow += step) {
        final RowMetaInterface inputRowMeta = inputRowMetas[idxInputRow];
        idxInputRowField = inputRowMeta.indexOfValue(outputFieldName);
        if (idxInputRowField > -1) {

          // get the value of the input row field
          inputRowFieldValue = inputRows[idxInputRow][idxInputRowField];

          // avoid null values if the InputRowSelectionPolicy requests it
          if (!(inputRowSelectionPolicy.avoidNull() && inputRowFieldValue == null)) {
            break; // exit the inner loop
          }
        }
      }

      if (idxInputRowField == -1) {
        // TODO(AR): error: could not find required output row field in any of the inputRows, should be reported?
        //  likely an impossible case if the output row meta was setup correctly
      }

      // set the output row field value to that of the found input row field value
      outputRow[idxOutputRowField] = inputRowFieldValue;
    }

    return outputRow;
  }

  /**
   * Remove an item from an Object array. This is a slow operation, later we want to just flag this object and discard
   * it at the next resize. The question is of-course if it makes that much of a difference in the end.
   *
   * @param objects
   * @param index
   * @return
   */
  public static Object[] removeItem( Object[] objects, int index ) {
    Object[] newObjects = new Object[objects.length - 1];
    System.arraycopy( objects, 0, newObjects, 0, index );
    System.arraycopy( objects, index + 1, newObjects, index, objects.length - index - 1 );
    return newObjects;
  }

  /**
   * Add two arrays and make one new one.
   *
   * @param one
   *          The first array
   * @param the
   *          length of the row data or of it's longer, the location of the new extra value in the returned data row
   * @param two
   *          The second array
   * @return a new Array containing all elements from one and two after one another
   */
  public static Object[] addRowData( Object[] one, int sourceLength, Object[] two ) {
    Object[] result = resizeArray( one, sourceLength + two.length );

    System.arraycopy( two, 0, result, sourceLength, two.length );

    return result;
  }

  /**
   * Add a single value to a row of data
   *
   * @param rowData
   *          The original row of data
   * @param the
   *          length of the row data or of it's longer, the location of the new extra value in the returned data row
   * @param extra
   *          The extra value to add
   * @return a new Array containing all elements, including the extra one
   */
  public static Object[] addValueData( Object[] rowData, int length, Object extra ) {

    Object[] result = resizeArray( rowData, length + 1 );
    result[length] = extra;
    return result;
  }

  /**
   * Remove a number of items in a row of data.
   *
   * @param rowData
   *          the row of data to remove from
   * @param index
   *          the index of all the items in the source table to remove. We don't check if the same index gets deleted
   *          twice!
   */
  public static Object[] removeItems( Object[] rowData, int[] index ) {
    Object[] data = new Object[rowData.length - index.length];

    int count = data.length - 1;
    int removenr = index.length - 1;
    for ( int i = rowData.length - 1; i >= 0; i-- ) {
      if ( removenr >= 0 && i == index[removenr] ) {
        removenr--;
      } else {
        data[count] = rowData[i];
        count--;
      }
    }

    return data;
  }
}
