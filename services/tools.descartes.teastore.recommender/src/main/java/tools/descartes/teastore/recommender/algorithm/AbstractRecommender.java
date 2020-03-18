/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tools.descartes.teastore.recommender.algorithm;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jersey.repackaged.com.google.common.collect.Lists;
import jersey.repackaged.com.google.common.collect.Maps;
import tools.descartes.teastore.entities.Order;
import tools.descartes.teastore.entities.OrderItem;
import tools.descartes.teastore.entities.Product;
import tools.descartes.teastore.entities.User;
import tools.descartes.teastore.recommender.algorithm.impl.cf.PreprocessedSlopeOneRecommender;
import tools.descartes.teastore.recommender.algorithm.impl.cf.SlopeOneRecommender;
import tools.descartes.teastore.recommender.algorithm.impl.orderbased.OrderBasedRecommender;
import tools.descartes.teastore.recommender.algorithm.impl.pop.PopularityBasedRecommender;
import tools.descartes.teastore.recommender.algorithm.util.RecommenderUtil;
import tools.descartes.teastore.recommender.monitoring.MonitoringConfiguration;
import tools.descartes.teastore.recommender.monitoring.MonitoringMetadata;
import tools.descartes.teastore.recommender.monitoring.ServiceParameters;
import tools.descartes.teastore.recommender.monitoring.ThreadMonitoringController;

/**
 * Abstract class for basic recommendation functionality.
 * 
 * @author Johannes Grohmann
 *
 */
public abstract class AbstractRecommender implements IRecommender {

	private static Map<Class<? extends IRecommender>, RecommenderEnum> recommenders = new HashMap<>();

	static {
		recommenders = new HashMap<Class<? extends IRecommender>, RecommenderEnum>();
		recommenders.put(PopularityBasedRecommender.class, RecommenderEnum.POPULARITY);
		recommenders.put(SlopeOneRecommender.class, RecommenderEnum.SLOPE_ONE);
		recommenders.put(PreprocessedSlopeOneRecommender.class, RecommenderEnum.PREPROC_SLOPE_ONE);
		recommenders.put(OrderBasedRecommender.class, RecommenderEnum.ORDER_BASED);
	}

	private boolean trainingFinished = false;

	/**
	 * Defines the maximum number of recommendations different implementations
	 * should return. Is NOT mandatory for any of the algorithms.
	 */
	public static final int MAX_NUMBER_OF_RECOMMENDATIONS = 10;

	private static final Logger LOG = LoggerFactory.getLogger(AbstractRecommender.class);

	/**
	 * This represents the matrix assigning each user a frequency for each product
	 * ID. The frequency resembles the number of times, a user has bought that item.
	 */
	private Map<Long, Map<Long, Double>> userBuyingMatrix;

	/**
	 * This set maps a userId to a set, containing the corresponding OrderItemSets,
	 * i.e. all orders that were done by the user.
	 */
	private Map<Long, Set<OrderItemSet>> userItemSets;

	/**
	 * This is an enumeration of all available products seen during the training
	 * phase.
	 */
	private Set<Long> totalProducts;

	/**
	 * Outsourced functionalities that are used for the training of the
	 * recommenders.
	 */
	private RecommenderUtil recommenderUtil = new RecommenderUtil();

	@Override
	public void train(List<OrderItem> orderItems, List<Order> orders) {
		// monitoring => parameters
		ServiceParameters serviceParameters = new ServiceParameters();
		serviceParameters.addNumberOfElements("items", orderItems.size());
		serviceParameters.addNumberOfElements("orders", orders.size());
		serviceParameters.addEnum("recommender", recommenders.get(this.getClass()));
		// end monitoring

		// set random session id -> this is a workaround because we do not have a real
		// session for the training
		ThreadMonitoringController.setSessionId(UUID.randomUUID().toString());
		
		// monitoring => enter train service
		ThreadMonitoringController.getInstance().enterService(MonitoringMetadata.SERVICE_TRAIN,
				MonitoringMetadata.ASSEMBLY_RECOMMENDER, serviceParameters);

		try {
			// monitoring
			long tic = System.currentTimeMillis();
			totalProducts = new HashSet<>();

			// 1. first create order mapping unorderized
			Map<Long, OrderItemSet> unOrderizeditemSets = new HashMap<>();
			for (OrderItem orderItem : orderItems) {
				recommenderUtil.processOrderItem(unOrderizeditemSets, orderItem, totalProducts);
			}
			// monitoring => log loop outer
			ThreadMonitoringController.getInstance().logLoopIterationCount(MonitoringMetadata.LOOP_ITEMS,
					orderItems.size());

			// 2. preprocess -> now map each id with the corresponding order
			userItemSets = preprocess(orders, unOrderizeditemSets);
			
			// 3. buy matrix
			try {
				userBuyingMatrix = recommenderUtil.createUserBuyingMatrix(userItemSets);
			} finally {
				ThreadMonitoringController.getInstance().exitService();
			}
			
			// 4. trainForRecommender -> delegate training
			long preprocStart = ThreadMonitoringController.getInstance().getTime();
			trainForRecommender();
			ThreadMonitoringController.getInstance().logResponseTime(MonitoringMetadata.INTERNAL_PREPROCESS,
					MonitoringMetadata.RESOURCE_CPU, preprocStart, MonitoringConfiguration.EVOLUTION_RECOGNIZED);

			LOG.info("Training recommender finished. Training took: " + (System.currentTimeMillis() - tic) + "ms.");
			trainingFinished = true;
		} finally {
			long overhead = ThreadMonitoringController.getInstance().exitService();

			// append = true
			try (PrintWriter output = new PrintWriter(
					new FileWriter("/Users/David/monitoring-teastore/overhead.txt", true))) {
				output.printf("%s\r\n", ";" + String.valueOf(overhead));
			} catch (Exception e) {
			}
		}
	}

	private Map<Long, Set<OrderItemSet>> preprocess(List<Order> orders, Map<Long, OrderItemSet> unOrderizeditemSets) {
		long startFind = ThreadMonitoringController.getInstance().getTime();

		Map<Long, Set<OrderItemSet>> ret = new HashMap<>();
		Map<Order, OrderItemSet> itemSets = new HashMap<>();

		for (Long orderid : unOrderizeditemSets.keySet()) {
			Order realOrder = findOrder(orders, orderid);
			itemSets.put(realOrder, unOrderizeditemSets.get(orderid));
		}

		for (Order order : itemSets.keySet()) {
			if (!userItemSets.containsKey(order.getUserId())) {
				userItemSets.put(order.getUserId(), new HashSet<OrderItemSet>());
			}
			itemSets.get(order).setUserId(order.getUserId());
			userItemSets.get(order.getUserId()).add(itemSets.get(order));
		}

		// monitoring
		ThreadMonitoringController.getInstance().logResponseTime(MonitoringMetadata.INTERNAL_CREATE_USET,
				MonitoringMetadata.RESOURCE_CPU, startFind);

		return ret;
	}

	/**
	 * Triggers implementing classes if they want to execute a pre-processing step
	 * during {@link AbstractRecommender#train(List, List)}.
	 */
	protected void trainForRecommender() {
		// do nothing
	}

	@Override
	public List<Long> recommendProducts(Long userid, List<OrderItem> currentItems, RecommenderEnum recommender)
			throws UnsupportedOperationException {
		if (!trainingFinished) {
			return Lists.newLinkedList();
		}
		if (currentItems.isEmpty()) {
			// if input is empty return empty list
			return new LinkedList<>();
		}

		List<Long> items = new ArrayList<>(currentItems.size());
		for (OrderItem item : currentItems) {
			items.add(item.getProductId());
		}
		List<Long> result = execute(userid, items);

		return result;
	}

	/**
	 * Filters the given ranking of recommendations and deletes items that already
	 * are in the cart. Furthermore caps the recommendations and only uses the
	 * {@link AbstractRecommender#MAX_NUMBER_OF_RECOMMENDATIONS} highest rated
	 * recommendations.
	 * 
	 * @param priorityList The unfiltered ranking assigning each recommended product
	 *                     ID a score or an importance. Does not need to be sorted.
	 * @param currentItems The list of item IDs that must NOT be contained in the
	 *                     returned list.
	 * @return A sorted list of recommendations with a size not greater than
	 *         {@link AbstractRecommender#MAX_NUMBER_OF_RECOMMENDATIONS}
	 */
	protected List<Long> filterRecommendations(Map<Long, Double> priorityList, List<Long> currentItems) {
		TreeMap<Double, List<Long>> ranking = createRanking(priorityList);
		List<Long> reco = new ArrayList<>(MAX_NUMBER_OF_RECOMMENDATIONS);
		for (Double score : ranking.descendingKeySet()) {
			List<Long> productIds = ranking.get(score);
			for (long productId : productIds) {
				if (reco.size() < MAX_NUMBER_OF_RECOMMENDATIONS) {
					if (!currentItems.contains(productId)) {
						reco.add(productId);
					}
				} else {
					return reco;
				}
			}
		}
		return reco;
	}

	private TreeMap<Double, List<Long>> createRanking(Map<Long, Double> map) {
		if (map == null) {
			return Maps.newTreeMap();
		}
		// transforming the map into a treemap (for efficient access)
		TreeMap<Double, List<Long>> ranking = new TreeMap<Double, List<Long>>();
		for (Entry<Long, Double> entry : map.entrySet()) {
			List<Long> productIds = ranking.get(entry.getValue());
			if (productIds == null) {
				productIds = new ArrayList<>();
				ranking.put(entry.getValue(), productIds);
			}
			productIds.add(entry.getKey());
		}
		return ranking;
	}

	/**
	 * Has to be implemented by subclasses in order to perform actual
	 * recommendation.
	 * 
	 * @param userid       The id of the {@link User} to recommend for. May be null.
	 * @param currentItems A list containing all ids of {@link OrderItem}s.
	 * @return List of all IDs of the {@link Product} entities that are recommended
	 *         to add to the cart. Does not contain any {@link Product} that is
	 *         already part of the given list of {@link OrderItem}s. Might be empty.
	 */
	protected abstract List<Long> execute(Long userid, List<Long> currentItems);

	private Order findOrder(List<Order> orders, long orderid) {
		for (Order order : orders) {
			if (order.getId() == orderid) {
				return order;
			}
		}
		return null;
	}

	/**
	 * @return the userBuyingMatrix
	 */
	public Map<Long, Map<Long, Double>> getUserBuyingMatrix() {
		return userBuyingMatrix;
	}

	/**
	 * @param userBuyingMatrix the userBuyingMatrix to set
	 */
	public void setUserBuyingMatrix(Map<Long, Map<Long, Double>> userBuyingMatrix) {
		this.userBuyingMatrix = userBuyingMatrix;
	}

	/**
	 * @return the totalProducts
	 */
	public Set<Long> getTotalProducts() {
		return totalProducts;
	}

	/**
	 * @param totalProducts the totalProducts to set
	 */
	public void setTotalProducts(Set<Long> totalProducts) {
		this.totalProducts = totalProducts;
	}

	/**
	 * @return the userItemSets
	 */
	public Map<Long, Set<OrderItemSet>> getUserItemSets() {
		return userItemSets;
	}

	/**
	 * @param userItemSets the userItemSets to set
	 */
	public void setUserItemSets(Map<Long, Set<OrderItemSet>> userItemSets) {
		this.userItemSets = userItemSets;
	}

}
