/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.innometrics.integration.app.recommender.repository.impl.datamodel;

import static org.slf4j.LoggerFactory.getLogger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.mahout.cf.taste.common.NoSuchItemException;
import org.apache.mahout.cf.taste.common.NoSuchUserException;
import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveArrayIterator;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.AbstractDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericItemPreferenceArray;
import org.apache.mahout.cf.taste.impl.model.GenericPreference;
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.slf4j.Logger;

import com.google.common.collect.Lists;

/**
 * An in-memory {@link org.apache.mahout.cf.taste.model.DataModel}, based on {@link org.apache.mahout.cf.taste.impl.model.GenericDataModel}, but which
 * allows to set and delete preferences without rebuilding it from scratch. <br>
 * 
 * This model is very similar to {@link org.apache.mahout.cf.taste.impl.model.GenericDataModel}, but stores
 * {@link #userIDs} and {@link #itemIDs} in mutable {@link org.apache.mahout.cf.taste.impl.common.FastIDSet}s, and
 * implements {@link #setPreference()} and {@link #removePreference()}
 * 
 * @author renaud@apache.org
 */
@SuppressWarnings("serial")
public final class MutableDataModel extends AbstractDataModel {
	private static final Logger LOG = getLogger(MutableDataModel.class);

	private final FastIDSet userIDs;
	private final FastByIDMap<PreferenceArray> preferenceFromUsers;
	private final FastIDSet itemIDs;
	private final FastByIDMap<PreferenceArray> preferenceForItems;
	private final FastByIDMap<FastByIDMap<Long>> timestamps;

	/**
	 * Creates a new empty {@link org.apache.mahout.cf.taste.model.DataModel}.
	 */
	public MutableDataModel() {
		this(null, null);
	}

	/**
	 * <p>
	 * Creates a new from the given users (and their preferences). This
	 * {@link org.apache.mahout.cf.taste.model.DataModel} retains all this information in memory.
	 * </p>
	 * 
	 * @param userData
	 *            users to include; (see also
	 *            {@link #toDataMap(org.apache.mahout.cf.taste.impl.common.FastByIDMap, boolean)})
	 */
	public MutableDataModel(FastByIDMap<PreferenceArray> userData) {
		this(userData, null);
	}

	/**
	 * <p>
	 * Creates a new from the given users (and their preferences). This
	 * {@link org.apache.mahout.cf.taste.model.DataModel} retains all this information in memory.
	 * </p>
	 * 
	 * @param userData
	 *            users to include; (see also
	 *            {@link #toDataMap(org.apache.mahout.cf.taste.impl.common.FastByIDMap, boolean)})
	 * @param timestamps
	 *            optionally, provided timestamps of preferences as milliseconds
	 *            since the epoch. User IDs are mapped to maps of item IDs to
	 *            Long timestamps.
	 */
	public MutableDataModel(FastByIDMap<PreferenceArray> userData,
			FastByIDMap<FastByIDMap<Long>> timestamps) {

		if (userData == null)
			userData = new FastByIDMap<PreferenceArray>();

		this.preferenceFromUsers = userData;
		FastByIDMap<Collection<Preference>> prefsForItems = new FastByIDMap<Collection<Preference>>();
		FastIDSet itemIDSet = new FastIDSet();
		int currentCount = 0;
		float maxPrefValue = Float.NEGATIVE_INFINITY;
		float minPrefValue = Float.POSITIVE_INFINITY;
		for (Map.Entry<Long, PreferenceArray> entry : preferenceFromUsers
				.entrySet()) {
			PreferenceArray prefs = entry.getValue();
			prefs.sortByItem();
			for (Preference preference : prefs) {
				long itemID = preference.getItemID();
				itemIDSet.add(itemID);
				Collection<Preference> prefsForItem = prefsForItems.get(itemID);
				if (prefsForItem == null) {
					prefsForItem = Lists.newArrayListWithCapacity(2);
					prefsForItems.put(itemID, prefsForItem);
				}
				prefsForItem.add(preference);
				float value = preference.getValue();
				if (value > maxPrefValue) {
					maxPrefValue = value;
				}
				if (value < minPrefValue) {
					minPrefValue = value;
				}
			}
			if (++currentCount % 10000 == 0) {
				LOG.info("Processed {} users", currentCount);
			}
		}
		LOG.info("Processed {} users", currentCount);

		setMinPreference(minPrefValue);
		setMaxPreference(maxPrefValue);

		this.itemIDs = itemIDSet;

		this.preferenceForItems = GenericDataModel.toDataMap(prefsForItems,
				false);

		for (Map.Entry<Long, PreferenceArray> entry : preferenceForItems
				.entrySet()) {
			entry.getValue().sortByUser();
		}

		this.userIDs = new FastIDSet();
		LongPrimitiveIterator it = userData.keySetIterator();
		while (it.hasNext()) {
			userIDs.add(it.next());
		}

		this.timestamps = timestamps;
	}

	@Override
	public LongPrimitiveArrayIterator getUserIDs() {
		// TODO maybe cache it?
		return new LongPrimitiveArrayIterator(userIDs.toArray());
	}

	/**
	 * @throws org.apache.mahout.cf.taste.common.NoSuchUserException
	 *             if there is no such user
	 */
	@Override
	public PreferenceArray getPreferencesFromUser(long userID)
			throws NoSuchUserException {
		PreferenceArray prefs = preferenceFromUsers.get(userID);
		if (prefs == null) {
			throw new NoSuchUserException(userID);
		}
		return prefs;
	}

	@Override
	public FastIDSet getItemIDsFromUser(long userID) throws TasteException {
		PreferenceArray prefs = getPreferencesFromUser(userID);
		int size = prefs.length();
		FastIDSet result = new FastIDSet(size);
		for (int i = 0; i < size; i++) {
			result.add(prefs.getItemID(i));
		}
		return result;
	}

	@Override
	public LongPrimitiveArrayIterator getItemIDs() {
		// TODO maybe cache it?
		return new LongPrimitiveArrayIterator(itemIDs.toArray());
	}

	@Override
	public PreferenceArray getPreferencesForItem(long itemID)
			throws NoSuchItemException {
		PreferenceArray prefs = preferenceForItems.get(itemID);
		if (prefs == null) {
			throw new NoSuchItemException(itemID);
		}
		return prefs;
	}

	@Override
	public Float getPreferenceValue(long userID, long itemID)
			throws TasteException {
		PreferenceArray prefs = getPreferencesFromUser(userID);
		int size = prefs.length();
		for (int i = 0; i < size; i++) {
			if (prefs.getItemID(i) == itemID) {
				return prefs.getValue(i);
			}
		}
		return null;
	}

	@Override
	public Long getPreferenceTime(long userID, long itemID)
			throws TasteException {
		if (timestamps == null) {
			return null;
		}
		FastByIDMap<Long> itemTimestamps = timestamps.get(userID);
		if (itemTimestamps == null) {
			throw new NoSuchUserException(userID);
		}
		return itemTimestamps.get(itemID);
	}

	@Override
	public int getNumItems() {
		return itemIDs.size();
	}

	@Override
	public int getNumUsers() {
		return userIDs.size();
	}

	@Override
	public int getNumUsersWithPreferenceFor(long itemID) {
		PreferenceArray prefs1 = preferenceForItems.get(itemID);
		return prefs1 == null ? 0 : prefs1.length();
	}

	@Override
	public int getNumUsersWithPreferenceFor(long itemID1, long itemID2) {
		PreferenceArray prefs1 = preferenceForItems.get(itemID1);
		if (prefs1 == null) {
			return 0;
		}
		PreferenceArray prefs2 = preferenceForItems.get(itemID2);
		if (prefs2 == null) {
			return 0;
		}

		int size1 = prefs1.length();
		int size2 = prefs2.length();
		int count = 0;
		int i = 0;
		int j = 0;
		long userID1 = prefs1.getUserID(0);
		long userID2 = prefs2.getUserID(0);
		while (true) {
			if (userID1 < userID2) {
				if (++i == size1) {
					break;
				}
				userID1 = prefs1.getUserID(i);
			} else if (userID1 > userID2) {
				if (++j == size2) {
					break;
				}
				userID2 = prefs2.getUserID(j);
			} else {
				count++;
				if (++i == size1 || ++j == size2) {
					break;
				}
				userID1 = prefs1.getUserID(i);
				userID2 = prefs2.getUserID(j);
			}
		}
		return count;
	}

	@Override
	public void removePreference(long userID, long itemID) {

		userIDs.remove(userID);
		itemIDs.remove(itemID);

		// User preferences
		List<Long> usersToRemoveInItems = new ArrayList<Long>();
		if (preferenceFromUsers.containsKey(userID)) {
			for (Preference p : preferenceFromUsers.get(userID)) {
				usersToRemoveInItems.add(p.getItemID());
			}
		}
		preferenceFromUsers.remove(userID);

		// Item preferences
		List<Long> itemsToRemoveInUsers = new ArrayList<Long>();
		if (preferenceForItems.containsKey(itemID)) {
			for (Preference p : preferenceForItems.get(itemID)) {
				itemsToRemoveInUsers.add(p.getUserID());
			}
		}
		preferenceForItems.remove(itemID);

		// 2nd round on Item preferences to remove refs to Users
		for (Long item : itemsToRemoveInUsers) {
			if (preferenceForItems.containsKey(item)) {
				List<Preference> newPi = new ArrayList<Preference>();
				for (Preference p : preferenceForItems.get(item)) {
					if (p.getUserID() != userID) {
						newPi.add(p);
					}
				}
				preferenceForItems.put(item, new GenericItemPreferenceArray(
						newPi));
			}
		}

		// 2nd round on User preferences to remove refs to Items
		for (Long user : usersToRemoveInItems) {
			if (preferenceFromUsers.containsKey(user)) {
				List<Preference> newPu = new ArrayList<Preference>();
				for (Preference p : preferenceFromUsers.get(user)) {
					if (p.getItemID() != itemID) {
						newPu.add(p);
					}
				}
				preferenceFromUsers.put(user, new GenericUserPreferenceArray(
						newPu));
			}
		}
	}

	@Override
	public void setPreference(long userID, long itemID, float value) {

		userIDs.add(userID);
		itemIDs.add(itemID);

		setMinPreference(Math.min(getMinPreference(), value));
		setMaxPreference(Math.max(getMaxPreference(), value));

		Preference p = new GenericPreference(userID, itemID, value);

		// User preferences
		GenericUserPreferenceArray newUPref;
		if (preferenceFromUsers.containsKey(userID)) {
			PreferenceArray oldPref = preferenceFromUsers.get(userID);
			newUPref = new GenericUserPreferenceArray(oldPref.length() + 1);
			for (int i = 0; i < oldPref.length(); i++) {
				newUPref.set(i, oldPref.get(i));
			}
			newUPref.set(oldPref.length(), p);
		} else {
			newUPref = new GenericUserPreferenceArray(1);
			newUPref.set(0, p);
		}
		preferenceFromUsers.put(userID, newUPref);

		// Item preferences
		GenericItemPreferenceArray newIPref;
		if (preferenceForItems.containsKey(itemID)) {
			PreferenceArray oldPref = preferenceForItems.get(itemID);
			newIPref = new GenericItemPreferenceArray(oldPref.length() + 1);
			for (int i = 0; i < oldPref.length(); i++) {
				newIPref.set(i, oldPref.get(i));
			}
			newIPref.set(oldPref.length(), p);
		} else {
			newIPref = new GenericItemPreferenceArray(1);
			newIPref.set(0, p);
		}
		preferenceForItems.put(itemID, newIPref);
	}

	@Override
	public void refresh(Collection<Refreshable> alreadyRefreshed) {
		// Does nothing
	}

	@Override
	public boolean hasPreferenceValues() {
		return true;
	}
}
