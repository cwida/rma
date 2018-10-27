/**
 * Copyright (C) 2018 Dean De Leo, email: dleo[at]cwi.nl
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#ifndef PMA_DENSITY_BOUNDS_HPP_
#define PMA_DENSITY_BOUNDS_HPP_

#include <cassert>
#include <type_traits>
#include <utility>
#include <vector>

namespace pma {

/**
 * Determine the density limits for a `Packed Memory Array' structure:
 * - [rho_0, rho_h] determine the lower densities
 * - [theta_h, theta_0] determine the higher densities
 *
 * There are some constraints to respect:
 * 0 <= rho_0 < rho_h < theta_h < theta_0 <= 1 : the natural order among densities
 * 2 * rho_h < (<=?) theta_h : logic constraint for resizing, since C > theta_h => 2*C > rho_h
 */
struct DensityBounds {
    const double rho_0; // lower density at the lowest level of the tree
    const double rho_h; // lower density at the highest level of the tree
    const double theta_h; // upper density at the highest level of the tree
    const double theta_0; // upper density at the lowest level of the tree

    /**
     * Automatically fetch the density bounds from the configuration/console params
     * It assumes the function pma::initialise(), in driver.hpp, has already been invoked.
     */
    DensityBounds();

    /**
     * Set the density bounds
     */
    DensityBounds(double rho_0, double rho_h, double theta_h, double theta_0);


    template<typename T>
    std::enable_if_t< std::is_integral_v<T>, std::pair<double, double> > thresholds(int tree_height, T current_height) const {
        return thresholds_int(tree_height, static_cast<int>(current_height));
    }
    template<typename T>
    std::enable_if_t< std::is_floating_point_v<T>, std::pair<double, double> > thresholds(int tree_height, T current_height) const {
        return thresholds_dec(tree_height, static_cast<double>(current_height));
    }

private:
    std::pair<double, double> thresholds_int(int tree_height, int current_height) const noexcept;
    std::pair<double, double> thresholds_dec(int tree_height, double current_height) const noexcept;
};

/**
 * Cache for the density computations, to avoid recomputing for each level in the calibrator tree
 */
class CachedDensityBounds {
    DensityBounds m_density_bounds; // container of the actually density bounds (rho/theta.)
    std::vector<std::pair<double, double>> m_cached_densities; // cached densities

    /**
     * Recompute the cached densities
     */
    void rebuild_cached_densities(int tree_height);

public:
    /**
     * Initialise the density bounds according to the configuration/console params.
     * It assumes the function pma::initialise(), in driver.hpp, has already been invoked.
     */
    CachedDensityBounds();

    /**
     * Explicitly set the density bounds
     */
    CachedDensityBounds(double rho_0, double rho_h, double theta_h, double theta_0);

    /**
     * Retrieve the bounds for the given height, assuming that the height of the calibrator
     * tree is the same of the last call
     * @param current_height: the height of the given level, in [1, tree_height]
     */
    std::pair<double, double> thresholds(int current_height) const noexcept;

    /**
     * Retrieve the bounds for the given height
     * @param tree_height: the total height of the tree, starting from 1
     * @param current_height: the height of the given level, in [1, tree_height]
     */
    std::pair<double, double> thresholds(int tree_height, int current_height) noexcept;

    /**
     * Retrieve the max density for the root of the calibrator tree, that is \theta_h
     */
    double get_upper_threshold_root() const noexcept;

    /**
     * Retrieve the max density for the leaves of the calibrator tree, that is \theta_0
     */
    double get_upper_threshold_leaves() const noexcept;

    /**
     * Retrieve the underlying thresholds
     */
    const DensityBounds& densities() const noexcept;

    /**
     * Get the height of the current calibrator tree
     */
    int get_calibrator_tree_height() const noexcept;
};

inline std::pair<double, double> CachedDensityBounds::thresholds(int tree_height, int current_height) noexcept {
    assert(1 <= current_height && current_height <= tree_height);
    if(static_cast<int>(m_cached_densities.size()) != tree_height) rebuild_cached_densities(tree_height);
    return m_cached_densities[current_height -1];
}

inline std::pair<double, double> CachedDensityBounds::thresholds(int current_height) const noexcept {
    assert(1 <= current_height && current_height <= m_cached_densities.size());
    return m_cached_densities[current_height -1];
}

} // namespace pma

#endif /* PMA_DENSITY_BOUNDS_HPP_ */
