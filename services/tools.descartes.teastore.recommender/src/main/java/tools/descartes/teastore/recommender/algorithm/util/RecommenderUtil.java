package tools.descartes.teastore.recommender.algorithm.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import tools.descartes.teastore.entities.OrderItem;
import tools.descartes.teastore.recommender.algorithm.OrderItemSet;
import tools.descartes.teastore.recommender.monitoring.MonitoringMetadata;
import tools.descartes.teastore.recommender.monitoring.ServiceParameters;
import tools.descartes.teastore.recommender.monitoring.ThreadMonitoringController;

public class RecommenderUtil {
	
	private int k = 0;
	
	public void processOrderItem(Map<Long, OrderItemSet> unOrderizeditemSets, OrderItem orderItem, Set<Long> totalProducts) {
		long startOrder = ThreadMonitoringController.getInstance().getTime();
		
		if (!unOrderizeditemSets.containsKey(orderItem.getOrderId())) {
			unOrderizeditemSets.put(orderItem.getOrderId(), new OrderItemSet());
			unOrderizeditemSets.get(orderItem.getOrderId()).setOrderId(orderItem.getOrderId());
		}
		unOrderizeditemSets.get(orderItem.getOrderId()).getOrderset().put(orderItem.getProductId(),
				orderItem.getQuantity());
		// see, if we already have our item
		if (!totalProducts.contains(orderItem.getProductId())) {
			// if not known yet -> add
			totalProducts.add(orderItem.getProductId());
		}
		
		// monitoring => keep the overhead feasible
		if (k++ % 500 == 0) {
			ThreadMonitoringController.getInstance().logResponseTime(MonitoringMetadata.INTERNAL_ITEM_PROCESS,
					MonitoringMetadata.RESOURCE_CPU, startOrder);
		}
	}
	
	/**
	 * Transforms the list of orders into one matrix containing all user-IDs and
	 * their number of buys (i.e., their rating) of all product-IDs. A
	 * quantity/rating of a user is null, if the user did not buy that item. If the
	 * user bought one item at least once, the contained value (rating) is the
	 * number of times, he bought one given item.
	 * 
	 * @param useritemsets A map assigning each user-ID all its OrderItemSets
	 * @return A Map representing a matrix of each user-ID assigning each item-ID
	 *         its number of buys (as double value)
	 */
	public Map<Long, Map<Long, Double>> createUserBuyingMatrix(Map<Long, Set<OrderItemSet>> useritemsets) {
		ServiceParameters paraInner = new ServiceParameters();
		paraInner.addNumberOfElements("userItems", useritemsets.size());
		ThreadMonitoringController.getInstance().enterService(MonitoringMetadata.SERVICE_BUY_MATRIX,
				MonitoringMetadata.ASSEMBLY_UTIL, paraInner);
		
		long startMatrix = ThreadMonitoringController.getInstance().getTime();

		Map<Long, Map<Long, Double>> matrix = new HashMap<>();
		// for each user
		for (Entry<Long, Set<OrderItemSet>> entry : useritemsets.entrySet()) {
			// create a new line for this user-ID
			Map<Long, Double> line = new HashMap<>();
			// for all orders of that user
			for (OrderItemSet orderset : entry.getValue()) {
				// for all orderitems of that orderset
				for (Entry<Long, Integer> product : orderset.getOrderset().entrySet()) {
					// if key was not known before -> first occurence
					if (!line.containsKey(product.getKey())) {
						line.put(product.getKey(), Double.valueOf(product.getValue()));
					} else {
						// if key was known before -> increase counter
						line.put(product.getKey(), Double.valueOf(line.get(product.getKey()) + product.getValue()));
					}
				}
			}
			// add this user-ID to the matrix
			matrix.put(entry.getKey(), line);
		}
		ThreadMonitoringController.getInstance().logResponseTime(MonitoringMetadata.INTERNAL_MATRIX,
				MonitoringMetadata.RESOURCE_CPU, startMatrix);

		return matrix;
	}

}
