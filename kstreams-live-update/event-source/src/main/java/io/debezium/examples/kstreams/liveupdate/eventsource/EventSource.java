/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.examples.kstreams.liveupdate.eventsource;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Random;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Triggers change events by persisting {@link Order} records.
 *
 * @author Gunnar Morling
 */
class EventSource {

    private static final Logger LOG = LoggerFactory.getLogger(EventSource.class);

    private boolean running = true;
    private Thread thread;
    private final Random random = new Random();

    public void run() {
        thread = new Thread(() -> {
            EntityManagerFactory entityManagerFactory = Persistence.createEntityManagerFactory("templatePU");
            EntityManager entityManager = entityManagerFactory.createEntityManager();

            entityManager.getTransaction().begin();
            List<Category> categories = entityManager.createQuery("from Category c", Category.class).getResultList();
            Object[] minMaxCustomerIds = (Object[]) entityManager.createNativeQuery("select min(id), max(id) from customers").getSingleResult();
            Object[] minMaxProductIds = (Object[]) entityManager.createNativeQuery("select min(id), max(id) from products").getSingleResult();

            entityManager.getTransaction().commit();

            int i = 0;
            while (running) {
                if (i % 50 == 0) {
                    entityManager.getTransaction().begin();
                }

                entityManager.persist(getRandomOrder(entityManager, (int)minMaxCustomerIds[0], (int)minMaxCustomerIds[1], (int)minMaxProductIds[0], (int)minMaxProductIds[1], categories));

                i++;
                try {
                    Thread.sleep(50);
                }
                catch (InterruptedException e) {
                    LOG.info("Interrupted");
                    running = false;
                }

                if (i % 50 == 0) {
                    LOG.info("Inserted {} orders", i);
                    entityManager.getTransaction().commit();
                    entityManager.clear();
                }
            }

            LOG.info("Clean-up");

            entityManager.close();
            entityManagerFactory.close();
        });

        thread.start();
    }

    private Order getRandomOrder(EntityManager entityManager, int minCustomerId, int maxCustomerId, int minProductId, int maxProductId, List<Category> categories) {
        Category category = categories.get(random.nextInt(categories.size()));
        int customerId = minCustomerId + random.nextInt(maxCustomerId - minCustomerId + 1);
        int productId = minProductId + random.nextInt(maxProductId - minProductId + 1);
        int quantity = random.nextInt(4) + 1;

        return new Order(
                ZonedDateTime.now(),
                customerId,
                productId,
                entityManager.getReference(Category.class, category.id),
                quantity,
                category.getRandomPrice()
        );
    }

    public void stop() {
        try {
            thread.interrupt();
            thread.join();
        }
        catch (InterruptedException e) {
        }
    }
}