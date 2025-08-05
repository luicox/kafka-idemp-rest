package com.luico.quarkus.consumer;

import io.agroal.api.AgroalDataSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.sql.SQLException;


@ApplicationScoped
public class TrackingRepository {

    @Inject
    AgroalDataSource dataSource;

    public boolean isAlreadyProcessed(String eventId) {
        try (var conn = dataSource.getConnection();
             var stmt = conn.prepareStatement("SELECT 1 FROM event_tracking WHERE event_id = ? AND status = 'COMPLETED'")) {
            stmt.setString(1, eventId);
            return stmt.executeQuery().next();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("Error checking tracking", e);
        }
    }

    public void saveAsInProgress(String eventId) {
        try (var conn = dataSource.getConnection();
             var stmt = conn.prepareStatement(
                     "INSERT INTO event_tracking (event_id, status) VALUES (?, 'IN_PROGRESS') ON DUPLICATE KEY UPDATE status = 'IN_PROGRESS'"
             )) {
            stmt.setString(1, eventId);
            stmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("Error saving IN_PROGRESS", e);
        }
    }

    public void markAsCompleted(String eventId) {
        try (var conn = dataSource.getConnection();
             var stmt = conn.prepareStatement(
                     "UPDATE event_tracking SET status = 'COMPLETED' WHERE event_id = ?"
             )) {
            stmt.setString(1, eventId);
            stmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("Error updating COMPLETED", e);
        }
    }
}
