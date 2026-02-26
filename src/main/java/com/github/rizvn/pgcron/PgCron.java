package com.github.rizvn.pgcron;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PgCron {
    private DataSource dataSource;

    public PgCron(DataSource dataSource) {
        this.dataSource = dataSource;
    }


    public void schedule(String jobName, String schedule, String command) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement("SELECT cron.schedule(?, ?, ?)")) {
            stmt.setString(1, jobName);
            stmt.setString(2, schedule);
            stmt.setString(3, command);
            stmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException("failed to schedule cron job: " + e.getMessage(), e);
        }
    }

    public void remove(String jobName) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement("SELECT cron.unschedule(?)")) {
            stmt.setString(1, jobName);
            stmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException("failed to remove cron job: " + e.getMessage(), e);
        }
    }

    public void pause(String jobName) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                 "SELECT cron.alter_job((SELECT jobid FROM cron.job WHERE jobname = ?), active := false)")) {
            stmt.setString(1, jobName);
            stmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException("failed to pause cron job: " + e.getMessage(), e);
        }
    }
}

