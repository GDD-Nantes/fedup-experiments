package fr.gdd.sage.generics;

import fr.gdd.sage.interfaces.BackendIterator;
import fr.gdd.sage.interfaces.RandomIterator;
import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.interfaces.SPOC;



/**
 * An iterator that enable retrieving values from the dictionary. Once
 * retrieved, the value is cached and only gets erased when the
 * underlying identifier changes.
 */
public class LazyIterator<ID, SKIP> implements BackendIterator<ID, SKIP>, RandomIterator {

    public BackendIterator<ID, SKIP> iterator;
    private Backend<ID, SKIP> backend;

    private ID subject_id = null;
    private ID predicate_id = null;
    private ID object_id = null;
    private ID context_id = null;

    private boolean subject_has_changed = true;
    private boolean predicate_has_changed = true;
    private boolean object_has_changed = true;
    private boolean context_has_changed = true;

    private String subject = null;
    private String predicate = null;
    private String object = null;
    private String context = null;

    public LazyIterator(Backend<ID, SKIP> backend, BackendIterator<ID, SKIP> wrapped) {
        this.backend = backend;
        this.iterator = wrapped;
    }

    @Override
    public ID getId(final int code) {
        switch (code) {
        case SPOC.SUBJECT:
            return this.subject_id;
        case SPOC.PREDICATE:
            return this.predicate_id;
        case SPOC.OBJECT:
            return this.object_id;
        case SPOC.CONTEXT:
            return this.context_id;
        }
        return null;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public void next() {
        iterator.next();

        if (iterator.getId(SPOC.SUBJECT) != this.subject_id) {
            this.subject_has_changed = true;
            this.subject_id = iterator.getId(SPOC.SUBJECT);
        }
        if (iterator.getId(SPOC.PREDICATE) != this.predicate) {
            this.predicate_has_changed = true;
            this.predicate_id = iterator.getId(SPOC.PREDICATE);
        }
        if (iterator.getId(SPOC.OBJECT) != this.object_id) {            
            this.object_has_changed = true;
            this.object_id = iterator.getId(SPOC.OBJECT);
        }
        if (iterator.getId(SPOC.CONTEXT) != this.context_id) {
            this.context_has_changed = true;
            this.context_id = iterator.getId(SPOC.CONTEXT);
        }
    };

    @Override
    public void reset() {
        iterator.reset();
        this.subject_id = null;
        this.predicate  = null;
        this.object_id  = null;
        this.context_id = null;
        this.subject_has_changed   = true;
        this.predicate_has_changed = true;
        this.object_has_changed    = true;
        this.context_has_changed   = true;
    }

    @Override
    public String getValue(final int code) {
        switch (code) {
        case SPOC.SUBJECT:
            if (subject_has_changed) {
                subject = backend.getValue(subject_id, code);
                subject_has_changed = false;
            }
            return subject;
        case SPOC.PREDICATE:
            if (predicate_has_changed) {
                predicate = backend.getValue(predicate_id, code);
                predicate_has_changed = false;
            }
            return predicate;
        case SPOC.OBJECT:
            if (object_has_changed) {
                object = backend.getValue(object_id, code);
                object_has_changed = false;
            }
            return object;
        case SPOC.CONTEXT:
            if (context_has_changed) {
                context = backend.getValue(context_id, code);
                context_has_changed = false;
            }
            return context;
        }
        return null;
    }

    @Override
    public SKIP current() {
        return iterator.current();
    }

    @Override
    public SKIP previous() {
        return iterator.previous();
    }

    @Override
    public void skip(SKIP to) {
        iterator.skip(to);
    }


    
    @Override
    public boolean random() {
        ((RandomIterator) iterator).random();
        return false;
    }

    @Override
    public long cardinality() {
        return ((RandomIterator) iterator).cardinality();
    }


}
