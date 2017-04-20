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

#include "index/directory_reader.hpp"
#include "store/fs_directory.hpp"
#include "search/term_filter.hpp"
#include "search/prefix_filter.hpp"
#include "search/boolean_filter.hpp"
#include "search/phrase_filter.hpp"
#include "search/bm25.hpp"

#include <boost/program_options.hpp>
#include <boost/chrono.hpp>
#include <boost/thread.hpp>
#include <unicode/uclean.h>

#include <random>
#include <regex>
#include <fstream>
#include <iostream>

namespace {

namespace po = boost::program_options;

}

static const std::string INDEX_DIR = "index-dir";
static const std::string OUTPUT = "out";
static const std::string INPUT = "in";
static const std::string MAX = "max-tasks";
static const std::string THR = "threads";
static const std::string TOPN = "topN";
static const std::string RND = "random";
static const std::string RPT = "repeat";
static const std::string CSV = "csv";

static bool v = false;

typedef std::unique_ptr<std::string> ustringp;

/**
 *
 */
struct Line {
    typedef std::shared_ptr<Line> ptr;
    std::string category;
    std::string text;

    Line(std::string& c, std::string& t) :
    category(c),
    text(t) {
    }
};

/**
 *
 */
struct Task {
    std::string category;
    std::string text;

    typedef std::shared_ptr<Task> ptr;

    irs::filter::prepared::ptr prepared;

    int taskId;
    int totalHitCount;
    int topN;

    boost::posix_time::time_duration tdiff;
    std::thread::id tid;

    Task(std::string& s, std::string& t, int n, irs::filter::prepared::ptr p) :
    category(s),
    text(t),
    prepared(p),
    taskId(0),
    totalHitCount(0),
    topN(n) {
    }

    virtual ~Task() {
    }

    void go(std::thread::id id, irs::directory_reader& reader) {
        tid = id;
        boost::posix_time::ptime start = boost::posix_time::microsec_clock::local_time();

        query(reader);

        tdiff = boost::posix_time::microsec_clock::local_time() - start;
    }

    virtual int query(irs::directory_reader& reader) = 0;

    virtual void print(std::ostream& out) = 0;
    virtual void print_csv(std::ostream& out) = 0;

};

/**
 *
 */
struct SearchTask : public Task {

    SearchTask(std::string& s, std::string& t, int n, irs::filter::prepared::ptr p) :
    Task(s, t, n, p) {
    }

    struct Entry {
        irs::doc_id_t id;
        float score;

        Entry(irs::doc_id_t i, float s) :
        id(i),
        score(s) {
        }
    };

    std::vector<Entry> top_docs;

    /**
     * 
     */
    virtual int query(irs::directory_reader& reader) override {
        int cnt = 0;
        for (auto& segment : reader) { // iterate segments
            irs::order order;
            order.add<irs::bm25_sort>(irs::string_ref::nil);
            auto prepared_order = order.prepare();
            irs::score_doc_iterator::ptr docs = prepared->execute(segment, prepared_order); // query segment
            auto& score = docs->attributes().get<iresearch::score>();

            /// 
            /// @todo sort by score and get only topN top documents
            ///  

            while (docs->next()) {
                ++totalHitCount;
                if (cnt < topN) {
                    irs::bytes_ref value;
                    const irs::doc_id_t doc_id = docs->value(); // get doc id
                    docs->score();
                    auto scoreValue = score ? score->get<float>(0) : .0; //score ? irs::bytes_ref(score->value()) : irs::bytes_ref::nil; 
                    //std::cout << "id=" << doc_id << "; score="<< scoreValue <<std::endl;
                    top_docs.emplace_back(doc_id, scoreValue);
                    ++cnt;
                }
            }
        }
        return 0;
    }

    /**
     */
    void print(std::ostream& out) override {
        out << "TASK: cat=" << category << " q='body:" << text << "' hits=" << totalHitCount << std::endl;
        out << "  " << tdiff.total_milliseconds() / 1000. << " msec" << std::endl;
        out << "  thread " << tid << std::endl;
        for (auto& doc : top_docs) {
            out << "  doc=" << doc.id << " score=" << doc.score << std::endl;
        }
        out << std::endl;
    }

    /**
     */
    void print_csv(std::ostream& out) override {
        out << category << "," << text << "," << totalHitCount << "," << tdiff.total_milliseconds() / 1000. << "," << tdiff.total_milliseconds() << std::endl;
    }

};

/**
 */
class TaskSource {
    std::atomic<int> idx;
    std::vector<Task::ptr> tasks;
    std::random_device rd;
    std::mt19937 g;

    /**
     *
     */
    int parseLines(std::string& line, Line::ptr& p) {
        static const std::regex m1("(\\S+): (.+)");

        std::smatch res;
        std::string category;
        std::string text;

        if (std::regex_match(line, res, m1)) {
            category.assign(res[1].first, res[1].second);
            text.assign(res[2].first, res[2].second);

            //        std::cout << category << " : " << text << std::endl;

            p = Line::ptr(new Line(category, text));

            return 0;

        }

        return -1;

    }

    /**
     *
     */
    int loadLines(std::vector<Line::ptr>& lines, std::istream& stream) {

        while (!stream.eof()) {
            std::string line;
            std::getline(stream, line);

            Line::ptr p;
            if (0 == parseLines(line, p)) {
                lines.push_back(p);
            }

        }

        return 0;
    }

    /**
     *
     */
    void shuffle(std::vector<Line::ptr>& line) {

        // @todo provide custom random?
        std::shuffle(line.begin(), line.end(), g);

    }

    /**
     *
     */
    static int pruneLines(std::vector<Line::ptr>& lines, std::vector<Line::ptr>& pruned_lines, int maxtasks) {

        std::map<std::string, int> cat_counts;

        for (auto& t : lines) {
            std::map<std::string, int>::iterator cat = cat_counts.find(t->category);
            int count = 0;
            if (cat != cat_counts.end()) {
                count = cat->second;
            }
            if (count < maxtasks) {
                ++count;
                if (cat != cat_counts.end()) {
                    cat->second = count;
                } else {
                    cat_counts[t->category] = count;
                }
                pruned_lines.push_back(t);
            }
        }

        return 0;
    }

    /**
     *
     */
    bool splitFreq(std::string& text, std::string& term) {
        static const std::regex freqPattern1("(\\S+)\\s*#\\s*(.+)"); // single term, prefix
        static const std::regex freqPattern2("\"(.+)\"\\s*#\\s*(.+)"); // phrase
        static const std::regex freqPattern3("((?:\\S+\\s+)+)\\s*#\\s*(.+)"); // AND/OR groups
        std::smatch res;

        if (std::regex_match(text, res, freqPattern1)) {
            term.assign(res[1].first, res[1].second);
            return true;
        } else if (std::regex_match(text, res, freqPattern2)) {
            term.assign(res[1].first, res[1].second);
            return true;
        } else if (std::regex_match(text, res, freqPattern3)) {
            term.assign(res[1].first, res[1].second);
            return true;
        }
        return false;
    }

    /**
     *
     */
    int prepareQueries(std::vector<Line::ptr>& lines, irs::directory_reader& reader, int topN) {

        //        static const std::regex filterPattern(" \\+filter=([0-9\\.]+)%");
        //        static const std::regex minShouldMatchPattern(" \\+minShouldMatch=(\\d+)($| )");

        irs::order order;
        order.add<irs::bm25_sort>(irs::string_ref::nil);
        auto ord = order.prepare();

        for (auto& line : lines) {

            irs::filter::prepared::ptr prepared = nullptr;
            std::string terms;

            if (line->category == "HighTerm" || line->category == "MedTerm" || line->category == "LowTerm") {
                if (splitFreq(line->text, terms)) {
                    irs::by_term query;
                    query.field("body").term(terms);
                    prepared = query.prepare(reader, ord);
                }
            } else if (line->category == "HighPhrase" || line->category == "MedPhrase" || line->category == "LowPhrase") {
                // @todo what's the difference between irs::by_phrase and irs::And?
                if (splitFreq(line->text, terms)) {
                    std::istringstream f(terms);
                    std::string term;
                    irs::by_phrase query;
                    query.field("body");
                    while (getline(f, term, ' ')) {
                        query.push_back(term);
                    }
                    prepared = query.prepare(reader, ord);
                }
            } else if (line->category == "AndHighHigh" || line->category == "AndHighMed" || line->category == "AndHighLow") {
                if (splitFreq(line->text, terms)) {
                    std::istringstream f(terms);
                    std::string term;
                    irs::And query;
                    while (getline(f, term, ' ')) {
                        irs::by_term& part = query.add<irs::by_term>();
                        part.field("body").term(term.c_str() + 1); // skip '+' at the start of the term
                    }
                    prepared = query.prepare(reader, ord);
                }
            } else if (line->category == "OrHighHigh" || line->category == "OrHighMed" || line->category == "OrHighLow") {
                if (splitFreq(line->text, terms)) {
                    std::istringstream f(terms);
                    std::string term;
                    irs::Or query;
                    while (getline(f, term, ' ')) {
                        irs::by_term& part = query.add<irs::by_term>();
                        part.field("body").term(term);
                    }
                    prepared = query.prepare(reader, ord);
                }
            } else if (line->category == "Prefix3") {
                irs::by_prefix query;
                terms.assign(line->text.begin(), line->text.end() - 1); // cut '~' at the end of the text
                query.field("body").term(terms);
                prepared = query.prepare(reader, ord);
            }

            if (prepared != nullptr) {
                tasks.emplace_back(new SearchTask(line->category, terms, topN, prepared));
                if (v) std::cout << tasks.size() << ": cat=" << line->category << "; term=" << terms << std::endl;
            }
        }

        if (v) std::cout << "Tasks prepared=" << tasks.size() << std::endl;

        return 0;
    }

    int repeatLines(std::vector<Line::ptr>& lines, std::vector<Line::ptr>& rep_lines, int repeat, bool do_shuffle) {
        while (repeat != 0) {
            if (do_shuffle) {
                shuffle(lines);
            }
            rep_lines.insert(std::end(rep_lines), std::begin(lines), std::end(lines));
            --repeat;
        }
        return 0;
    }

public:

    TaskSource() : idx(0), g(rd()) {

    }

    /**
     *
     */
    int load(std::istream& stream, int maxtasks, int repeat, irs::directory_reader& reader, int topN, bool do_shuffle) {
        /// 
        ///  this fn mimics lucene-util's LocalTaskSource behavior
        ///  -- many similar tasks generated 
        ///
        std::vector<Line::ptr> rep_lines;
        {
            std::vector<Line::ptr> pruned_lines;
            {
                std::vector<Line::ptr> lines;
                // parse all lines to category:text
                loadLines(lines, stream);
                // shuffle
                if (do_shuffle) {
                    shuffle(lines);
                }
                // prune tasks
                pruneLines(lines, pruned_lines, maxtasks);
            }
            // multiply pruned with shuffling
            repeatLines(pruned_lines, rep_lines, repeat, do_shuffle);
        }

        // prepare queries
        prepareQueries(rep_lines, reader, topN);

        return 0;
    }

    /**
     *
     */
    Task::ptr next() {
        int next = idx++; // atomic get and increment
        if (next < tasks.size()) {
            return tasks[next];
        }
        return nullptr;
    }

    /**
     *
     */
    std::vector<Task::ptr>& getTasks() {
        return tasks;
    }

};

/**
 *
 */
class TaskThread {
public:

    typedef std::shared_ptr<TaskThread> ptr;

    struct Args {
        TaskSource& tasks;
        irs::directory_reader& reader;

        Args(TaskSource& t,
                irs::directory_reader& r) :
        tasks(t),
        reader(r) {

        }
    };

private:


    std::thread* thr;

    /**
     * worker
     * @param s
     */
    void worker(Args a) {
        auto task = a.tasks.next();

        while (task != nullptr) {
            task->go(thr->get_id(), a.reader);
            task = a.tasks.next();
        }

    }


public:

    /**
     *
     * @param a
     */
    void start(Args a) {
        thr = new std::thread(std::bind(&TaskThread::worker, this, a));
    }

    /**
     *
     */
    void join() {
        thr->join();
        delete thr;
    }

};

/**
 */
static int testQueries(std::vector<Task::ptr>& tasks, irs::directory_reader& reader) {
    for (auto& segment : reader) { // iterate segments
        int cnt = 0;
        for (auto& task : tasks) {
            ++cnt;
            std::cout << "running query=" << cnt << std::endl;
            auto& query = task->prepared;
            auto docs = query->execute(segment); // query segment
            while (docs->next()) {
                const irs::doc_id_t doc_id = docs->value(); // get doc id
                std::cout << cnt << " : " << doc_id << std::endl;
            }
        }
    }
    return 0;
}

/**
 */
static int printResults(std::vector<Task::ptr>& tasks, std::ostream& out, bool csv) {
    for (auto& task : tasks) {
        csv ? task->print_csv(out) : task->print(out);
    }
    return 0;
}

/**
 */
static int search(const std::string& path, std::istream& in, std::ostream& out, 
        int maxtasks, int repeat, int thrs, int topN, bool shuffle, bool csv) {

    boost::posix_time::ptime start = boost::posix_time::microsec_clock::local_time();

    irs::fs_directory dir(path);
    irs::directory_reader reader = irs::directory_reader::open(dir, irs::formats::get("1_0"));

    TaskSource tasks;

    // prepare tasks set
    tasks.load(in, maxtasks, repeat, reader, topN, shuffle);
    std::cout << "TASK LEN=" << tasks.getTasks().size() << std::endl;

    // threads
    std::vector<TaskThread::ptr> tthreads;
    for (int i = 0; i < thrs; ++i) {
        TaskThread::ptr task = TaskThread::ptr(new TaskThread());
        task->start(TaskThread::Args(tasks, reader));
        tthreads.push_back(task);
    }

    // join
    while (!tthreads.empty()) {
        tthreads.back()->join();
        tthreads.pop_back();
    }

    // run search in
    //testQueries(tasks.getTasks(), reader);
    printResults(tasks.getTasks(), out, csv);

    boost::posix_time::time_duration msdiff = boost::posix_time::microsec_clock::local_time() - start;

    std::cout << msdiff.total_milliseconds() << " msec total" << std::endl;

    u_cleanup();

    return 0;
}

/**
 *
 * @param vm
 * @return
 */
static int search(const po::variables_map& vm) {
    if (!vm.count(INDEX_DIR) || !vm.count(INPUT)) {
        return 1;
    }

    auto& path = vm[INDEX_DIR].as<std::string>();

    if (path.empty()) {
        return 1;
    }

    int maxtasks = 1;
    if (vm.count(MAX)) {
        maxtasks = vm[MAX].as<int>();
    }
    std::cout << "Max tasks in category=" << maxtasks << std::endl;

    int repeat = 20;
    if (vm.count(RPT)) {
        repeat = vm[RPT].as<int>();
    }
    std::cout << "Task repeat count=" << repeat << std::endl;

    bool shuffle = false;
    if (vm.count(RND)) {
        shuffle = true;
    }
    std::cout << "Do task list shuffle=" << shuffle << std::endl;

    int thrs = 1;
    if (vm.count(THR)) {
        thrs = vm[THR].as<int>();
    }
    std::cout << "Search threads=" << thrs << std::endl;

    int topN = 10;
    if (vm.count(TOPN)) {
        topN = vm[TOPN].as<int>();
    }
    std::cout << "Number of top documents to collect=" << topN << std::endl;

    bool csv = false;
    if (vm.count(CSV)) {
        csv = true;
    }
    std::cout << "Output CSV=" << shuffle << std::endl;

    auto& file = vm[INPUT].as<std::string>();
    std::fstream in(file, std::fstream::in);
    if (!in) {

        return 1;
    }

    if (vm.count(OUTPUT)) {
        auto& file = vm[OUTPUT].as<std::string>();
        std::fstream out(file, std::fstream::out | std::fstream::trunc);
        if (!out) {
            return 1;
        }

        return search(path, in, out, maxtasks, repeat, thrs, topN, shuffle, csv);
    }

    return search(path, in, std::cout, maxtasks, repeat, thrs, topN, shuffle, csv);
}

/**
 *
 * @param argc
 * @param argv
 * @return
 */
int main(int argc, char* argv[]) {
    //    irs::logger::output_le(iresearch::logger::IRL_ERROR, stderr);

    po::variables_map vm;

    // general description
    std::string mode;
    po::options_description desc("\n[IReSearch-benchmarks-search] General options");
    desc.add_options()
            ("help,h", "produce help message")
            ("mode,m", po::value<std::string>(&mode), "Select mode: search");

    // stats mode description
    po::options_description put_desc("Search mode options");
    put_desc.add_options()
            (INDEX_DIR.c_str(), po::value<std::string>(), "Path to index directory")
            (INPUT.c_str(), po::value<std::string>(), "Task file")
            (OUTPUT.c_str(), po::value<std::string>(), "Stats file")
            (MAX.c_str(), po::value<int>(), "Maximum tasks per category")
            (RPT.c_str(), po::value<int>(), "Task repeat count")
            (THR.c_str(), po::value<int>(), "Number of search threads")
            (TOPN.c_str(), po::value<int>(), "Number of top search results")
            (RND.c_str(), "Shuffle tasks")
            (CSV.c_str(), "CSV output");

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

    // enter dump mode
    if ("search" == mode) {
        desc.add(put_desc);
        po::store(po::parse_command_line(argc, argv, desc), vm);

        return search(vm);
    }

    return 0;
}
