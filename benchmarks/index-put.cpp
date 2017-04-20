//
// IResearch search engine 
// 
// Copyright (c) 2017 by EMC Corporation, All Rights Reserved
// 
// This software contains the intellectual property of EMC Corporation or is licensed to
// EMC Corporation from third parties. Use of this software and the intellectual property
// contained therein is expressly limited to the terms and conditions of the License
// Agreement under which it is provided by or on behalf of EMC.
// 

#include "index/index_writer.hpp"
#include "analysis/token_streams.hpp"
#include "analysis/text_token_stream.hpp"
#include "store/fs_directory.hpp"
#include "analysis/token_attributes.hpp"

#include <boost/program_options.hpp>
#include <boost/thread.hpp>
#include <boost/chrono.hpp>
#include <unicode/uclean.h>

#include <fstream>
#include <iostream>

namespace {

namespace po = boost::program_options;

}

static const std::string INDEX_DIR = "index-dir";
static const std::string OUTPUT = "out";
static const std::string INPUT = "in";
static const std::string MAX = "max-lines";
static const std::string THR = "threads";
static const std::string CPR = "commit-period";

typedef std::unique_ptr<std::string> ustringp;

#if defined(_MSC_VER) && (_MSC_VER < 1900)
  #define snprintf _snprintf
#endif

/**
 * Document
 */
struct Doc {
    static std::atomic<uint64_t> next_id;

    /**
     * C++ version 0.4 char* style "itoa":
     * Written by Lukás Chmela
     * Released under GPLv3.
     */
    char* itoa(int value, char* result, int base) {
        // check that the base if valid
        if (base < 2 || base > 36) {
            *result = '\0';
            return result;
        }

        char* ptr = result, *ptr1 = result, tmp_char;
        int tmp_value;

        do {
            tmp_value = value;
            value /= base;
            *ptr++ = "zyxwvutsrqponmlkjihgfedcba9876543210123456789abcdefghijklmnopqrstuvwxyz" [35 + (tmp_value - value * base)];
        } while (value);

        // Apply negative sign
        if (tmp_value < 0) *ptr++ = '-';
        *ptr-- = '\0';
        while (ptr1 < ptr) {
            tmp_char = *ptr;
            *ptr-- = *ptr1;
            *ptr1++ = tmp_char;
        }
        return result;
    }

    /**
     */
    struct Field {
        std::string& _name;
        const irs::flags feats;

        Field(std::string& n, const irs::flags& flags) : _name(n), feats(flags) {
        }

        const std::string& name() const {
            return _name;
        }

        float_t boost() const {
            return 1.0;
        }

        virtual irs::token_stream& get_tokens() const = 0;

        const irs::flags& features() const {
            return feats;
        }

        virtual bool write(irs::data_output& out) const = 0;
        
        virtual ~Field() {}
    };
    
    /**
     */
    struct StringField : public Field {
        std::string f;
        mutable irs::string_token_stream _stream;

        StringField(std::string& n, const irs::flags& flags, std::string& a) :
        Field(n, flags),
        f(a) {
        }

        irs::token_stream& get_tokens() const override {
            _stream.reset(f);
            return _stream;
        }

        bool write(irs::data_output& out) const override {
            irs::write_string(out, f.c_str(), f.length());
            return true;
        }
    };

    /**
     */
    struct TextField : public Field {
        std::string f;
        mutable irs::analysis::analyzer::ptr stream;
        static const std::string& aname;
        static const std::string& aignore;

        TextField(std::string& n, const irs::flags& flags, std::string& a) :
        Field(n, flags),
        f(a) {
            stream = irs::analysis::analyzers::get(aname, aignore);
        }

        irs::token_stream& get_tokens() const override {
            stream->reset(f);
            return *stream;
        }

        bool write(irs::data_output& out) const override {
            irs::write_string(out, f.c_str(), f.length());
            return true;
        }
        
        ~TextField() {
        }
    };

    /**
     */
    struct NumericField : public Field {
        mutable irs::numeric_token_stream stream;
        int64_t value;

        NumericField(std::string& n, irs::flags flags, uint64_t v) :
        Field(n, flags),
        value(v) {
        }

        irs::token_stream& get_tokens() const override {
            stream.reset(value);
            return stream;
        }

        bool write(irs::data_output& out) const override {
            irs::write_zvlong(out, value);
            return true;
        }
    };

    std::vector<Field*> elements;
    std::vector<Field*> store;
    
    ~Doc() {
        for (auto e : elements) {
            delete e;
        }
        for (auto e : store) {
            delete e;
        }
    }

    /**
     * Parse line to fields
     * @todo way too many string copies here
     * @param line
     * @return 
     */
    void fill(std::string* line) {
        static std::string n_id = "id";
        static std::string n_title = "title";
        static std::string n_date = "date";
        static std::string n_timesecnum = "timesecnum";
        static std::string n_body = "body";
        std::stringstream lineStream(*line);
        std::string cell;

        // id: uint64_t to string, base 36
        uint64_t id = next_id++; // atomic fetch and get
        char str[10];
        itoa(id, str, 36);
        char str2[10];
        snprintf(str2, sizeof (str2), "%6s", str);
        std::string s(str2);
        std::replace(s.begin(), s.end(), ' ', '0');
        elements.push_back(new StringField(n_id, irs::flags{irs::granularity_prefix::type()}, s));
        store.push_back(new StringField(n_id, irs::flags{irs::granularity_prefix::type()}, s));

        // title: string
        std::getline(lineStream, cell, '\t');
        elements.push_back(new StringField(n_title, irs::flags::empty_instance(), cell));

        // date: string
        std::getline(lineStream, cell, '\t');
        elements.push_back(new StringField(n_date, irs::flags::empty_instance(), cell));
        store.push_back(new StringField(n_date, irs::flags::empty_instance(), cell));

        // +date: uint64_t
        uint64_t t = 0; //boost::posix_time::microsec_clock::local_time().total_milliseconds();
        elements.push_back(new NumericField(n_timesecnum,
                irs::flags{irs::granularity_prefix::type()}, t));

        // body: text
        std::getline(lineStream, cell, '\t');
        elements.push_back(new TextField(n_body,
                irs::flags{irs::frequency::type(), irs::position::type(), irs::offset::type(), irs::norm::type()}, cell));
    }

};

std::atomic<uint64_t> Doc::next_id(0);
const std::string& Doc::TextField::aname = std::string("text");
const std::string& Doc::TextField::aignore = std::string("{\"locale\":\"en\", \"ignored_words\":[\"abc\", \"def\", \"ghi\"]}");

        
/**
 * Line file reader
 */
class LineFileReader {
public:

    /**
     * worker args
     */
    struct Args {
        std::istream& stream;
        int maxlines;

        Args(std::istream& s, int mx) :
        stream(s), maxlines(mx) {
        }
    };

    struct Stats {
        uint64_t lines;

        Stats() :
        lines(0) {
        }
    };

private:
    std::thread* thr;
    std::mutex mutex;
    std::condition_variable cond;
    std::queue<ustringp> queue;
    bool depleted;
    Stats stats;

    /**
     * worker
     * @param s
     */
    void worker(Args a) {

        {
            std::string line;
            getline(a.stream, line); // skip header
        }

        while (a.maxlines > 0) {
            auto line = ustringp(new std::string());
            std::getline(a.stream, *line);
            {
                std::lock_guard<std::mutex> l(mutex);
                //std::cout << "push: "<< (void*)line->c_str() << std::endl;
                queue.push(std::move(line));
                if (queue.back()->empty()) {
                    break;
                }
                cond.notify_one();
            }
            --a.maxlines;
        }

        {
            std::lock_guard<std::mutex> l(mutex);
            depleted = true;
            cond.notify_all();
        }
    }


public:

    /**
     * 
     */
    LineFileReader() : thr(nullptr), depleted(false) {
    }

    /**
     * 
     * @param a
     */
    void start(Args a) {
        thr = new std::thread(std::bind(&LineFileReader::worker, this, a));
    }

    /**
     * 
     */
    void join() {
        thr->join();
        delete thr;
    }

    /**
     * 
     * @return 
     */
    ustringp getLine() {
        ustringp r = nullptr;
        std::unique_lock<std::mutex> l(mutex);
        while (!depleted || !queue.empty()) {
            if (!queue.empty()) {
                r = std::move(queue.front());
                queue.pop();
                //std::cout << "pop: "<< (void*)r->c_str() << std::endl;
                break;
            }
            cond.wait(l);
        }
        return r;
    }

    /**
     * 
     * @return 
     */
    Stats& getStats() {
        return stats;
    }

};

/**
 * Synchronized committer
 */
class Committer {
    irs::index_writer::ptr writer;
    uint64_t inserts;
    int workers;
    uint64_t period;
    std::mutex mutex;

public:

    Committer(irs::index_writer::ptr w, int thrs, uint64_t cpr) :
    writer(w),
    inserts(0),
    workers(thrs),
    period(cpr) {
    }

    /**
     * 
     * @return 
     */
    bool commitPeriodic() {
        bool do_commit = false;
        {
            std::lock_guard<std::mutex> l(mutex);
            ++inserts;
            if (inserts > period) {
                do_commit = true;
                inserts = 0;
            }
        }
        if (do_commit) {
            writer->commit();
        }
        return do_commit;
    }

    /**
     * 
     * @return 
     */
    bool commitFinalize() {
        bool do_commit = false;
        {
            std::lock_guard<std::mutex> l(mutex);
            --workers;
            if (workers == 0 && inserts != 0) {
                do_commit = true;
            }
        }
        if (do_commit) {
            writer->commit();
        }
        return do_commit;
    }

};

/**
 * Indexing thread
 */
class Indexer {
public:

    /**
     * worker args
     */
    struct Args {
        LineFileReader& reader;
        irs::index_writer::ptr writer;
        Committer& committer;

        Args(LineFileReader& lr, irs::index_writer::ptr w, Committer& c) : reader(lr), writer(w), committer(c) {
        }
    };

    struct Stats {
        uint64_t inserts;
        uint64_t commits;

        Stats() :
        inserts(0),
        commits(0) {
        }
    };

private:
    std::thread* thr;
    std::mutex mutex;
    Stats stats;

    /**
     * worker
     * @param a
     */
    void worker(Args a) {
        while (true) {
            Doc doc;
            ustringp line = a.reader.getLine();
            if (line == nullptr || line->empty()) break;
            doc.fill(line.get());

            auto inserter = [&doc](const irs::index_writer::document& builder) {
              for (auto* field : doc.elements) {
                builder.insert<irs::Action::INDEX>(*field);
              }

              for (auto* field : doc.store) {
                builder.insert<irs::Action::STORE>(*field);
              }

              return false; // break the loop
            };

            a.writer->insert(inserter);
            ++stats.inserts;
            //std::cout << "insert: "<< (void*)line->c_str() << std::endl;
            if (a.committer.commitPeriodic()) {
                ++stats.commits;
                std::cout << "thread " << thr->get_id() << ": triggered commitPeriodic" << std::endl;

            }
        }
        // finalize
        if (a.committer.commitFinalize()) {
            ++stats.commits;
            std::cout << "thread " << thr->get_id() << ": triggered commitFinalize" << std::endl;
        }
        std::cout << "thread " << thr->get_id() << ": inserted docs=" << std::dec << stats.inserts <<
                ": commits=" << std::dec << stats.commits << std::endl;
    }

public:

    /**
     * 
     */
    Indexer() : thr(nullptr) {
    }

    /**
     * 
     * @param a
     */
    void start(Args a) {
        thr = new std::thread(std::bind(&Indexer::worker, this, a));
    }

    /**
     * 
     */
    void join() {
        thr->join();
        delete thr;
    }

    /**
     * 
     * @return 
     */
    Stats& getStats() {
        return stats;
    }
};

/**
 * Threads spawner
 * @param path
 * @param stream
 * @param maxlines
 * @param thrs
 * @return 
 */
static int put(const std::string& path, std::istream& stream, int maxlines, int thrs, int cpr) {

    boost::posix_time::ptime start = boost::posix_time::microsec_clock::local_time();

    irs::fs_directory dir(path);
    auto writer = irs::index_writer::make(dir, irs::formats::get("1_0"), irs::OPEN_MODE::OM_CREATE);

    Committer committer(writer, thrs, cpr);

    LineFileReader rl;
    rl.start(LineFileReader::Args(stream, maxlines));

    std::vector<Indexer*> indexers;

    while (thrs) {
        Indexer* indexer = new Indexer();
        indexer->start(Indexer::Args(rl, writer, committer));
        indexers.push_back(indexer);
        --thrs;
    }

    uint64_t inserts = 0;
    uint64_t commits = 0;
    while (!indexers.empty()) {
        indexers.back()->join();
        inserts += indexers.back()->getStats().inserts;
        commits += indexers.back()->getStats().commits;
        delete indexers.back();
        indexers.pop_back();
    }

    rl.join();

    writer->close();

    boost::posix_time::time_duration msdiff = boost::posix_time::microsec_clock::local_time() - start;

    std::cout << "Total inserts=" << inserts << "; commits=" << commits << "; ms=" << msdiff.total_milliseconds() << std::endl;

    u_cleanup();

    return 0;
}

int put(const std::string& path, std::istream& stream, int maxlines, int thrs, int cpr, size_t batch_size) {
  irs::fs_directory dir(path);
  auto writer = irs::index_writer::make(dir, irs::formats::get("1_0"), irs::OPEN_MODE::OM_CREATE);
  size_t inserter_count = std::max(1, thrs);
  size_t commit_ms = std::max(1, cpr);
  size_t line_max = std::max(1, maxlines);
  irs::async_utils::thread_pool thread_pool(inserter_count + 1 + 1); // +1 for commiter thread +1 for stream reader thread

  SCOPED_TIMER("Total Time");
  std::cout << "Data path=" << path << "; max-lines=" << maxlines << "; threads=" << thrs << "; commit-period=" << cpr << std::endl;

  struct {
    std::condition_variable cond_;
    std::atomic<bool> done_;
    bool eof_;
    std::mutex mutex_;
    std::vector<std::string> buf_;

    bool swap(std::vector<std::string>& buf) {
      SCOPED_LOCK_NAMED(mutex_, lock);

      for (;;) {
        buf_.swap(buf);
        buf_.resize(0);
        cond_.notify_all();

        if (!buf.empty()) {
          return true;
        }

        if (eof_) {
          done_.store(true);
          return false;
        }

        if (!eof_) {
          SCOPED_TIMER("Stream read wait time");
          cond_.wait(lock);
        }
      }
    }
  } batch_provider;

  batch_provider.done_.store(false);
  batch_provider.eof_ = false;

  // stream reader thread
  thread_pool.run([&batch_provider, &line_max, batch_size, &stream]()->void {
    SCOPED_TIMER("Stream read total time");
    SCOPED_LOCK_NAMED(batch_provider.mutex_, lock);

    while (line_max) {
      batch_provider.buf_.resize(batch_provider.buf_.size() + 1);

      auto& line = batch_provider.buf_.back();

      if (std::getline(stream, line).eof()) {
        batch_provider.buf_.pop_back();
        break;
      }

      --line_max;

      if (batch_provider.buf_.size() >= batch_size) {
        SCOPED_TIMER("Stream read idle time");
        batch_provider.cond_.wait(lock);
      }
    }

    batch_provider.eof_ = true;
  });

  // commiter thread
  thread_pool.run([&batch_provider, commit_ms, &writer]()->void {
    while (!batch_provider.done_.load()) {
      {
        SCOPED_TIMER("Commit time");
        writer->commit();
      }

      std::this_thread::sleep_for(std::chrono::milliseconds(commit_ms));
    }
  });

  // inderxer threads
  for (size_t i = inserter_count; i; --i) {
    thread_pool.run([&batch_provider, &writer]()->void {
      std::vector<std::string> buf;

      while (batch_provider.swap(buf)) {
        SCOPED_TIMER(std::string("Index batch ") + std::to_string(buf.size()));
        size_t i = 0;
        auto inserter = [&buf, &i](const irs::index_writer::document& builder) {
          Doc doc;

          doc.fill(&(buf[i]));

          for (auto* field: doc.elements) {
            builder.insert<irs::Action::INDEX>(*field);
          }

          for (auto* field : doc.store) {
            builder.insert<irs::Action::STORE>(*field);
          }

          return ++i < buf.size();
        };

        writer->insert(inserter);
      }
    });
  }

  thread_pool.stop();

  {
    SCOPED_TIMER("Commit time");
    writer->commit();
  }

  u_cleanup();

  return 0;
}

/**
 * 
 * @param vm
 * @return 
 */
int put(const po::variables_map& vm) {
    if (!vm.count(INDEX_DIR)) {
        return 1;
    }

    auto& path = vm[INDEX_DIR].as<std::string>();

    if (path.empty()) {
        return 1;
    }

    int maxlines = -1;
    if (vm.count(MAX)) {
        maxlines = vm[MAX].as<int>();
    }

    int thrs = 1;
    if (vm.count(THR)) {
        thrs = vm[THR].as<int>();
    }

    int cpr = -1;
    if (vm.count(CPR)) {
        cpr = vm[CPR].as<int>();
    }

    if (vm.count(INPUT)) {
        auto& file = vm[INPUT].as<std::string>();
        std::fstream in(file, std::fstream::in);
        if (!in) {
            return 1;
        }

        return put(path, in, maxlines, thrs, cpr);

    }

    return put(path, std::cin, maxlines, thrs, cpr);
}

/**
 * 
 * @param argc
 * @param argv
 * @return 
 */
int main(int argc, char* argv[]) {
    irs::logger::output_le(iresearch::logger::IRL_ERROR, stderr);

    po::variables_map vm;

    // general description
    std::string mode;
    po::options_description desc("\n[IReSearch-benchmarks-index] General options");
    desc.add_options()
            ("help,h", "produce help message")
            ("mode,m", po::value<std::string>(&mode), "Select mode: put");

    // stats mode description
    po::options_description put_desc("Put mode options");
    put_desc.add_options()
            (INDEX_DIR.c_str(), po::value<std::string>(), "Path to index directory")
            (INPUT.c_str(), po::value<std::string>(), "Input file")
            (MAX.c_str(), po::value<int>(), "Maximum lines")
            (THR.c_str(), po::value<int>(), "Number of insert threads")
            (CPR.c_str(), po::value<int>(), "Commit period in lines");

    po::command_line_parser parser(argc, argv);
    parser.options(desc).allow_unregistered();
    po::parsed_options options = parser.run();
    po::store(options, vm);
    po::notify(vm);

    // show help
    if (vm.count("help")) {
        desc.add(put_desc);
        std::cout << desc << std::endl;
        return 0;
    }

    irs::timer_utils::init_stats(true);
    auto output_stats = irs::make_finally([]()->void {
      irs::timer_utils::visit([](const std::string& key, size_t count, size_t time)->bool {
        std::cout << key << " calls:" << count << ", time: " << time/1000 << " us, avg call: " << time/1000/(double)count << " us"<< std::endl;
        return true;
      });
    });

    // enter dump mode
    if ("put" == mode) {
        desc.add(put_desc);
        po::store(po::parse_command_line(argc, argv, desc), vm);

        return put(vm);
    }

    return 0;
}
