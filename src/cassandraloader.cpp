#include "cassandraloader.h"

#include <sstream>
#include <thread>

#include <glog/logging.h>

#include "util.h"

CassandraLoader::CassandraLoader(const std::string& contact_points,
                                 const std::string& table,
                                 std::vector<std::string> versions,
                                 uint workers) :
        versions_(std::move(versions)),
        table_(table)
{
    cluster_ = cass_cluster_new();
    session_ = cass_session_new();
    cass_cluster_set_num_threads_io(cluster_, workers);
    cass_cluster_set_contact_points(cluster_, contact_points.c_str());
    retry_policy_ = cass_retry_policy_downgrading_consistency_new();
    cass_cluster_set_retry_policy(cluster_, retry_policy_);
    connect_thread_ = std::make_unique<std::thread>([&]{
        CassFuture* connect_future = nullptr;
        while (!connected_) {
            connect_future = cass_session_connect(session_, cluster_);
            if (cass_future_error_code(connect_future) != CASS_OK) {
                const char* message;
                size_t message_length;
                cass_future_error_message(connect_future, &message, &message_length);
                LOG(INFO) << "Unable to connect: " << message;
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                continue;
            }
            connected_ = true;
        }
        if (connect_future) {
            cass_future_free(connect_future);
        }
    });
}

CassandraLoader::~CassandraLoader() {
    if (connect_thread_) {
        connected_ = true;
        connect_thread_->join();
    }
    CassFuture* close_future = cass_session_close(session_);
    cass_future_wait(close_future);
    cass_future_free(close_future);
    cass_cluster_free(cluster_);
    cass_session_free(session_);
    cass_retry_policy_free(retry_policy_);
}

struct TaskWrapper {
    std::shared_ptr<LoadTask> task;
    TileId tile_id;
};

static void ResultCallback(CassFuture* future, void* data) {
    TaskWrapper* task_wrapper = static_cast<TaskWrapper*>(data);
    CassError result_error = cass_future_error_code(future);
    if(result_error == CASS_OK) {
        /* Retrieve result set and iterate over the rows */
        const CassResult* result = cass_future_get_result(future);
        CassIterator* rows = cass_iterator_from_result(result);
        if (cass_iterator_next(rows)) {
            const CassRow* row = cass_iterator_get_row(rows);
            const CassValue* value = cass_row_get_column_by_name(row, "tile");
            const char* tile_data;
            size_t tile_data_length;
            cass_value_get_string(value, &tile_data, &tile_data_length);
            std::string result_data;
            // TODO: eleminate copying
            util::decompress(tile_data, tile_data_length, result_data);
            Tile result_tile{task_wrapper->tile_id, std::move(result_data)};
            task_wrapper->task->SetResult(std::move(result_tile));
        } else {
            task_wrapper->task->NotifyError(LoadError::not_found);
        }
        cass_result_free(result);
        cass_iterator_free(rows);
    } else {
        const char* message;
        size_t message_length;
        cass_future_error_message(future, &message, &message_length);
        LOG(ERROR) << message;
        task_wrapper->task->NotifyError(LoadError::internal_error);

    }
    cass_future_free(future);
    delete task_wrapper;
}

void CassandraLoader::Load(std::shared_ptr<LoadTask> task, const TileId& tile_id,
                           const std::string& version) {
    if (!connected_) {
        task->NotifyError(LoadError::internal_error);
        return;
    }
    if (!HasVersion(version)) {
        task->NotifyError(LoadError::not_found);
        return;
    }
    int idx = xy_to_index(tile_id.x, tile_id.y);
    int block = idx / 32768;
    std::stringstream cql_statment;
    cql_statment << "SELECT tile FROM " << version << "." << table_
            << " WHERE idx=" << idx << " AND zoom=" << tile_id.z << " AND  block=" << block << ";";
    CassStatement* statement
      = cass_statement_new(cql_statment.str().c_str(), 0);
    cass_statement_set_consistency(statement, CASS_CONSISTENCY_ONE);

    CassFuture* result_future = cass_session_execute(session_, statement);
    cass_statement_free(statement);
    TaskWrapper* task_wrapper = new TaskWrapper{std::move(task), tile_id};
    cass_future_set_callback(result_future, &ResultCallback, static_cast<void*>(task_wrapper));
}

bool CassandraLoader::HasVersion(const std::string& version) const {
    for (const std::string& v : versions_) {
        if (v == version) {
            return true;
        }
    }
    return false;
}

int CassandraLoader::xy_to_index(int x, int y) {
    int mult = 1;
    int result = 0;
    while (x || y) {
        result += (mult * (x % 2));
        x = x / 2;
        mult *= 2;
        result += (mult * (y % 2));
        y = y / 2;
        mult *= 2;
    }
    return result;
}
