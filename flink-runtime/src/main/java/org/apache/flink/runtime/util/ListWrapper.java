/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * TODO: add javadoc.
 */
public class ListWrapper<T> implements List<T> {
	private final List<T> wrappedList;

	public ListWrapper(Collection<T> wrappedList) {
		this.wrappedList = new ArrayList<>(wrappedList.size());
		wrappedList.addAll(wrappedList);
	}

	@Override
	public int size() {
		return wrappedList.size();
	}

	@Override
	public boolean isEmpty() {
		return wrappedList.isEmpty();
	}

	@Override
	public boolean contains(Object o) {
		return wrappedList.contains(o);
	}

	@Override
	public Iterator<T> iterator() {
		return wrappedList.iterator();
	}

	@Override
	public Object[] toArray() {
		return wrappedList.toArray();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		return wrappedList.toArray(a);
	}

	@Override
	public boolean add(T T) {
		return wrappedList.add(T);
	}

	@Override
	public boolean remove(Object o) {
		return wrappedList.remove(o);
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		return wrappedList.containsAll(c);
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		return wrappedList.addAll(c);
	}

	@Override
	public boolean addAll(int index, Collection<? extends T> c) {
		return wrappedList.addAll(c);
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		return wrappedList.removeAll(c);
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		return wrappedList.retainAll(c);
	}

	@Override
	public void clear() {
		wrappedList.clear();
	}

	@Override
	public T get(int index) {
		return wrappedList.get(index);
	}

	@Override
	public T set(int index, T element) {
		return wrappedList.set(index, element);
	}

	@Override
	public void add(int index, T element) {
		wrappedList.add(index, element);
	}

	@Override
	public T remove(int index) {
		return wrappedList.remove(index);
	}

	@Override
	public int indexOf(Object o) {
		return wrappedList.indexOf(o);
	}

	@Override
	public int lastIndexOf(Object o) {
		return wrappedList.lastIndexOf(o);
	}

	@Override
	public ListIterator<T> listIterator() {
		return wrappedList.listIterator();
	}

	@Override
	public ListIterator<T> listIterator(int index) {
		return wrappedList.listIterator(index);
	}

	@Override
	public List<T> subList(int fromIndex, int toIndex) {
		return wrappedList.subList(fromIndex, toIndex);
	}
}
