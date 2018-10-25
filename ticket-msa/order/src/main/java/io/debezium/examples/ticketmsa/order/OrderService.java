package io.debezium.examples.ticketmsa.order;

import javax.enterprise.context.ApplicationScoped;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.transaction.Transactional;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import io.debezium.examples.ticketmsa.order.model.Order;

@Path("/orders")
@ApplicationScoped
public class OrderService {

    @PersistenceContext
    private EntityManager entityManager;

    @POST
    @Transactional
    @Produces("application/json")
    @Consumes("application/json")
    public Order addOrder(Order order) {
        order = entityManager.merge(order);
        return order;
    }
}
