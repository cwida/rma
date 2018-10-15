/*
 * driver.cpp
 *
 *  Created on: 17 Jan 2018
 *      Author: Dean De Leo
 */

#include "driver.hpp"

#include <sstream>

#include "configuration.hpp"
#include "console_arguments.hpp"
#include "errorhandling.hpp"
#include "factory.hpp"

#include "apma_distributions.hpp"
#include "sparse_uniform_distribution.hpp"
#include "uniform_distribution.hpp"
#include "zipf_distribution.hpp"

using namespace std;

namespace distribution {

void initialise(){
    /**
     * Uniform distribution
     */
    ADD_DISTRIBUTION("uniform", "Generate a permutation of [1, N] following a uniform distribution. No elements are repeated. The parameters alpha and beta are ignored.", [](){
        LOG_VERBOSE("Distribution: uniform");
        return make_uniform(ARGREF(int64_t, "num_inserts"));
    });
    ADD_DISTRIBUTION("sparse_uniform", "Generate a permutation in [1, beta], following a uniform distribution. No elements are repeated.", [](){
        int64_t beta = ARGREF(double, "beta").get();
        int64_t num_samples = ARGREF(int64_t, "num_inserts");
        uint64_t seed = ARGREF(uint64_t, "seed_random_permutation");
        LOG_VERBOSE("Distribution: sparse uniform(" << beta <<")");

        // sanity checks for the given parameters
        if(beta <= 1) RAISE_EXCEPTION(configuration::ConsoleArgumentError, "[sparse uniform distribution] "
                "The parameter --beta is either not set or invalid: " << beta);
        if(num_samples > beta) RAISE_EXCEPTION(configuration::ConsoleArgumentError, "[sparse uniform distribution] "
                "Invalid parameter --beta: " << beta << ". It's less than the number of insertions: " << num_samples);

        return make_sparse_uniform(1, beta, num_samples, seed);
    });

    /**
     * APMA distributions
     */
    ADD_DISTRIBUTION("apma_sequential", "Generate the sequential pattern [1, N]. The parameters alpha and beta are ignored.", [](){
        LOG_VERBOSE("Distribution: apma_sequential");
        return make_unique<SequentialForward>(1, ARGREF(int64_t, "num_inserts") +1); // [min, max)
    });
    ADD_DISTRIBUTION("apma_sequential_rev", "Generate the sequential pattern from N down to 1. The parameters alpha and beta are ignored.", [](){
        LOG_VERBOSE("Distribution: apma_sequential_rev");
        return make_unique<SequentialBackwards>(1, ARGREF(int64_t, "num_inserts") +1); // [min, max)
    });
    ADD_DISTRIBUTION("apma_bulk", "Generate sequential runs of size `N ^ alpha'. Each run is a contiguous and increasing sequence of integers. The parameter `alpha' determines the size of each run, in (0, 1]. The parameter beta is ignored.", [](){
        double alpha = ARGREF(double, "alpha");
        LOG_VERBOSE("Distribution: apma_bulk(" << alpha << ")");
        return make_unique<BulkForward>(ARGREF(int64_t, "num_inserts"), alpha);
    });
    ADD_DISTRIBUTION("apma_bulk_rev", "Generate sequential runs of size `N ^ alpha'. Each run is a contiguous and decreasing sequence of integers. The parameter `alpha' determines the size of each run, in (0, 1]. The parameter beta is ignored.", [](){
        double alpha = ARGREF(double, "alpha");
        LOG_VERBOSE("Distribution: apma_bulk_rev(" << alpha << ")");
        return make_unique<BulkForward>(ARGREF(int64_t, "num_inserts"), alpha);
    });
    ADD_DISTRIBUTION("apma_interleaved", "Generate multiple sequential runs of size 'N / alpha'. The elements are picked from each run in a round-robin fashion. The runs are contiguous and increasing. The parameter `alpha' (R in the APMA paper) determines the size of each run, in [0, Num_Inserts]. The parameter beta is ignored.", [](){
        size_t alpha = ARGREF(double, "alpha");
        LOG_VERBOSE("Distribution: apma_interleaved(" << alpha << ")");
        return make_unique<InterleavedForward>(ARGREF(int64_t, "num_inserts"), alpha);
    });
    ADD_DISTRIBUTION("apma_interleaved_rev", "Generate multiple sequential runs of size 'N / alpha'. The elements are picked from each run in a round-robin fashion. The runs are contiguous and decreasing. The parameter `alpha' (R in the APMA paper) determines the size of each run, in [0, Num_Inserts]. The parameter beta is ignored.", [](){
        size_t alpha = ARGREF(double, "alpha");
        LOG_VERBOSE("Distribution: apma_interleaved_rev(" << alpha << ")");
        return make_unique<InterleavedBackwards>(ARGREF(int64_t, "num_inserts"), alpha);
    });
    ADD_DISTRIBUTION("apma_noise", "Create a sequential run of size N * alpha, while the rest of the elements, N * (1 - alpha), are uniformly distributed. Each time, whether to extract from the sequential run or from the uniform bucket is decided randomly. The sequential run is contiguous and increasing. The parameters `alpha' determines the size of the sequential run: N*alpha, in (0, 1). The parameter beta is ignored.", [](){
        double alpha = ARGREF(double, "alpha");
        LOG_VERBOSE("Distribution: apma_noise(" << alpha << ")");
        return make_unique<NoiseForward>(ARGREF(int64_t, "num_inserts"), alpha);
    });
    ADD_DISTRIBUTION("apma_noise_rev", "As above, but the sequential run is decreasing, similarly to the APMA paper.", [](){
        double alpha = ARGREF(double, "alpha");
        LOG_VERBOSE("Distribution: apma_noise_rev(" << alpha << ")");
        return make_unique<NoiseBackwards>(ARGREF(int64_t, "num_inserts"), alpha);
    });

    ADD_DISTRIBUTION("zipf", "Zipf distribution. Parametric on `alpha' (real > 0) and `beta' (range size)", [](){
        double alpha = ARGREF(double, "alpha");
        double beta_dbl = ARGREF(double, "beta");
        if(alpha <= 0)
            RAISE_EXCEPTION(configuration::ConsoleArgumentError, "[zipf distribution] Invalid value for the parameter --alpha: " << alpha << ". It must be a floating value > 0");
        if(beta_dbl <= 0)
            RAISE_EXCEPTION(configuration::ConsoleArgumentError, "[zipf distribution] Invalid value for the parameter --beta: " << beta_dbl << ". It must be a value > 0");

        uint64_t beta = beta_dbl;
        LOG_VERBOSE("Distribution: zipf(" << alpha << ", " << beta << ")");
        uint64_t seed = ARGREF(uint64_t, "seed_random_permutation");
        return make_zipf(alpha, ARGREF(int64_t, "num_inserts"), beta, seed);
    });

    /**
     * Parameters
     */
    PARAMETER(double, "alpha").hint().set_default(0)
            .descr("Custom parameter, its semantic depends on the chosen distribution.");
    PARAMETER(double, "beta").hint().set_default(0)
            .descr("Custom parameter, its semantic depends on the chosen distribution.");

    { // the list of distributions
        stringstream helpstr;
        auto& distributions = factory().list();
        factory().sort_list();

        helpstr << "The distribution to use. The possible choices are: ";
        for(size_t i = 0; i < distributions.size(); i++){
            auto& e = distributions[i];
            helpstr << "\n- " << e->name() << ": " << e->description();
        }
        helpstr << "\n";

        PARAMETER(string, "distribution")["d"].hint()
                .descr(helpstr.str()).set_default("uniform")
                .validate_fn([&distributions](const std::string& distribution){
            auto res = find_if(begin(distributions), end(distributions), [&distribution](auto& impl){
                return impl->name() == distribution;
            });
            if(res == end(distributions))
                RAISE_EXCEPTION(configuration::ConsoleArgumentError, "Invalid distribution: " << distribution);
            return true;
        });
    }

}


std::unique_ptr<Distribution> generate_distribution() {
    return factory().make(ARGREF(string, "distribution"));
}

} // namespace distribution



