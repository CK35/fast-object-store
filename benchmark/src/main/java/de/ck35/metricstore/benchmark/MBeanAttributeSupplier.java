package de.ck35.metricstore.benchmark;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;

public class MBeanAttributeSupplier<T> implements Supplier<Optional<T>> {
	
	private static final Logger LOG = LoggerFactory.getLogger(MBeanAttributeSupplier.class);
	
	private final MBeanServer mBeanServer;
	private final ObjectName name;
	private final String attribute;
	private final Class<T> targetClass;

	public MBeanAttributeSupplier(MBeanServer mBeanServer, 
	                              ObjectName name, 
	                              String attribute,
	                              Class<T> targetClass) { 
		this.mBeanServer = mBeanServer;
		this.name = name;
		this.attribute = attribute;
		this.targetClass = targetClass;
	}
	
	@Override
	public Optional<T> get() {
		try {
			return Optional.fromNullable(targetClass.cast(mBeanServer.getAttribute(name, attribute)));
		} catch (JMException e) {
			LOG.debug("Could not load attribute: '{}' from MBean: '{}'!", attribute, name, e);
			return Optional.absent();
		}
	}

}