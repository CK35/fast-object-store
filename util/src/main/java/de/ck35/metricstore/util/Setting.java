package de.ck35.metricstore.util;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;

/**
 * Threadsafe setting implementation which holds a setting value and an
 * optional value which will be supplied when setting value is <code>null</code>.
 * This supplier will never return null because the default value is
 * not allowed to be <code>null</code>.
 * 
 * @param <T> Type of setting value.
 *
 * @author Christian Kaspari
 * @since 1.0.0
 */
public class Setting<T> implements Supplier<T> {

    private final T defaultValue;
    private final AtomicReference<T> valueReference;
 
    public Setting(T defaultValue) {
        this(defaultValue, null);
    }
    public Setting(T defaultValue, T optinalValue) {
        this.defaultValue = Objects.requireNonNull(defaultValue);
        this.valueReference = new AtomicReference<>(optinalValue);
    }
    @Override
    public T get() {
        return Optional.fromNullable(valueReference.get()).or(defaultValue);
    }
    public void set(T value) {
        this.valueReference.set(value);
    }
    public T getDefaultValue() {
        return defaultValue;
    }
}