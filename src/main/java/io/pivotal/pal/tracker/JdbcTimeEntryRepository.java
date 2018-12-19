package io.pivotal.pal.tracker;

import com.mysql.cj.jdbc.MysqlDataSource;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import javax.sql.DataSource;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;

public class JdbcTimeEntryRepository implements TimeEntryRepository {

    private JdbcTemplate jdbcTemplate;

    public JdbcTimeEntryRepository(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    public TimeEntry create(TimeEntry any) {
        // todo: return new TimeEntry(any);
        String query = "INSERT INTO time_entries(project_id, user_id, date, hours) VALUES(?,?,?,?)";
        KeyHolder keyHolder = new GeneratedKeyHolder();

        //= new PreparedStatementCreator();
        jdbcTemplate.update(
                con -> {
                    PreparedStatement stmt = con.prepareStatement(query, new String[]{"id"});
                    stmt.setLong(1, any.getProjectId());
                    stmt.setLong(2, any.getUserId());
                    stmt.setString(3, any.getDate().toString());
                    stmt.setLong(4, any.getHours());
                    return stmt;
                }, keyHolder);

        return this.find(((BigInteger) keyHolder.getKey()).longValue());

        //any.getProjectId(), any.getUserId(), any.getDate(), any.getHours(), keyHolder);
    }

    @Override
    public TimeEntry find(long timeEntryId) {
        try {
            return this.jdbcTemplate.queryForObject("SELECT * FROM time_entries where id=" + timeEntryId, new RowMapper<TimeEntry>() {
                @Override
                public TimeEntry mapRow(ResultSet rs, int rowNum) throws SQLException {

                    return new TimeEntry(rs.getLong(1), rs.getLong(2), rs.getLong(3),
                            LocalDate.parse(rs.getDate(4).toString()), rs.getInt(5));
                }
            });
        } catch (EmptyResultDataAccessException e) {

        }
        return null;
    }

    @Override
    public List<TimeEntry> list() {
        return this.jdbcTemplate.query("SELECT * FROM time_entries", (rs, i) -> {return new TimeEntry(rs.getLong(1), rs.getLong(2), rs.getLong(3),
                LocalDate.parse(rs.getDate(4).toString()), rs.getInt(5));});
    }

    @Override
    public TimeEntry update(long eq, TimeEntry any) {
        jdbcTemplate.update("UPDATE time_entries SET project_id=?, user_id=?, date=?, hours=? where id=?",any.getProjectId(),any.getUserId(),any.getDate(),any.getHours(), eq);
        return find(eq);
    }

    @Override
    public void delete(long timeEntryId) {
        jdbcTemplate.update("DELETE FROM time_entries where id=?",timeEntryId);
    }
}
