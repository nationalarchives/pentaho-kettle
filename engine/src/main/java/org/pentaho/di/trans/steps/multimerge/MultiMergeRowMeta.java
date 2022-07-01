/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2020 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.steps.multimerge;

import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;

/**
 * Extends {@link RowMeta} to provide an option
 * to prevent duplicate named columns across input
 * streams being copied to the output row.
 */
public class MultiMergeRowMeta extends RowMeta {

    private final boolean preventDuplicateFields;

    public MultiMergeRowMeta(final boolean preventDuplicateFields) {
        super();
        this.preventDuplicateFields = preventDuplicateFields;
    }

    @Override
    public void mergeRowMeta(final RowMetaInterface r, final String originStepName) {
        lock.writeLock().lock();
        try {
            if (preventDuplicateFields) {
                for (int x = 0; x < r.size(); x++) {
                    final ValueMetaInterface field = r.getValueMeta(x);
                    final String name = field.getName();
                    if (indexOfValue(name) == -1) {
                        addValueMeta(field);
                    }
                }
            } else {
                super.mergeRowMeta(r, originStepName);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

}
