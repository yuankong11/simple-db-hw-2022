package simpledb.storage;

public class TupleView extends Tuple {
    public TupleView(Tuple t) {
        super(t, false);
    }

    @Override
    public void setRecordId(RecordId rid) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setField(int i, Field f) {
        throw new UnsupportedOperationException();
    }
}
