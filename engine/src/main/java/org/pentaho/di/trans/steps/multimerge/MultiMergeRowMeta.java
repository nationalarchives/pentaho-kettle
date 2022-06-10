package org.pentaho.di.trans.steps.multimerge;

import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;

public class MultiMergeRowMeta extends RowMeta {

    public MultiMergeRowMeta(){
        super();
    }

    @Override
    public void mergeRowMeta(RowMetaInterface r, String originStepName ) {
        lock.writeLock().lock();
        try {
            for ( int x = 0; x < r.size(); x++ ) {
                ValueMetaInterface field = r.getValueMeta( x );
                if ( searchValueMeta( field.getName() ) == null ) {
                    addValueMeta( field ); // Not in list yet: add
                } else if(!field.getName().startsWith("CONST_")){
                    // We want to rename the field to Name[2], Name[3], ...
                    //
                    addValueMeta( renameValueMetaIfInRow( field, originStepName ) );
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }



}
