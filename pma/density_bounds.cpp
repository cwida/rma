/*
 * density_bounds.cpp
 *
 *  Created on: 22 Jan 2018
 *      Author: Dean De Leo
 */

#include "density_bounds.hpp"

#include "configuration.hpp"
#include "console_arguments.hpp"
#include "errorhandling.hpp"

#define CONF_ERROR(msg) RAISE_EXCEPTION(configuration::ConfigurationException, msg)

namespace pma {

/*****************************************************************************
 *                                                                           *
 *   DensityBounds                                                           *
 *                                                                           *
 *****************************************************************************/

// Density constraints
DensityBounds::DensityBounds() : DensityBounds(ARGREF(double, "rho_0"), ARGREF(double, "rho_h"), ARGREF(double, "theta_h"), ARGREF(double, "theta_0")){ }

DensityBounds::DensityBounds(double rho_0, double rho_h, double theta_h, double theta_0) :
    rho_0(rho_0), rho_h(rho_h), theta_h(theta_h), theta_0(theta_0){
    LOG_VERBOSE("PMA density thresholds: rho_0: " << rho_0 << ", rho_h: " << rho_h << ", theta_h: " << theta_h << ", theta_0: " << theta_0);

    // 0 <= rho_0 < rho_h < theta_h < theta_0 <= 1
    if(!(0 <= rho_0)) CONF_ERROR("Invalid densities: rho_0 < 0. rho_0: " << rho_0);
    if(!(rho_0 < rho_h)) CONF_ERROR("Invalid densities: rho_h <= rho_0. rho_0 is the density for the lowest level, rho_h the one for the highest level. It must hold 0 <= rho_0 < rho_h < theta_h < theta_0 <= 1. rho_0: " << rho_0 << ", rho_h: " << rho_h);
    if(!(rho_h <= theta_h)) CONF_ERROR("Invalid densities: theta_h <= rho_h. rho_h the lower density for the highest level, theta_h is the upper density for the highest level. It must hold 0 <= rho_0 < rho_h < theta_h < theta_0 <= 1. rho_h: " << rho_h << ", theta_h: " << theta_h);
    if(!(theta_h < theta_0)) CONF_ERROR("Invalid densities: theta_0 <= theta_0. theta_h is the upper density for the highest level, theta_0 the one for the lowest level. It must hold 0 <= rho_0 < rho_h < theta_h < theta_0 <= 1. theta_h: " << theta_h << ", theta_0: " << theta_0);
    if(!(theta_0 <= 1)) CONF_ERROR("Invalid densities: theta_0 > 1. theta_0: " << theta_0);

    // 2 * rho_h < theta_h
//    if(!(2*rho_h < theta_h)) CONF_ERROR("Invalid densities: 2 * rho_h >= theta_h. It must hold: 2*rho < theta_h (resizing constraint). rho_h: " << rho_h << ", theta_h: " << theta_h);
}

std::pair<double, double> DensityBounds::thresholds_int(int tree_height, int current_height) const noexcept {
    assert(1 <= current_height && current_height <= tree_height); // so levels = 1, 2, 3 => height = 3

    if(tree_height == 1) return {rho_0, theta_0}; // avoid dividing by 0, i.e. (tree_height -1))
    const double scale = static_cast<double>(tree_height - current_height) / static_cast<double>(tree_height -1);

    // compute delta
    double delta_r = rho_h - rho_0;
    double rho = rho_h - delta_r * scale;

    // compute theta
    double delta_t = theta_0 - theta_h;
    double theta = theta_h + delta_t * scale;

    return {rho, theta};
}

std::pair<double, double> DensityBounds::thresholds_dec(int tree_height, double current_height) const noexcept {
    assert(1 <= current_height && current_height <= tree_height); // so levels = 1, 2, 3 => height = 3

    if(tree_height == 1) return {rho_0, theta_0}; // avoid dividing by 0, i.e. (tree_height -1))
    const double scale = (static_cast<double>(tree_height) - current_height) / static_cast<double>(tree_height -1);

    // compute delta
    double delta_r = rho_h - rho_0;
    double rho = rho_h - delta_r * scale;

    // compute theta
    double delta_t = theta_0 - theta_h;
    double theta = theta_h + delta_t * scale;

    return {rho, theta};
}


/*****************************************************************************
 *                                                                           *
 *   CachedDensityBounds                                                     *
 *                                                                           *
 *****************************************************************************/
CachedDensityBounds::CachedDensityBounds() { }
CachedDensityBounds::CachedDensityBounds(double rho_0, double rho_h, double theta_h, double theta_0) :
        m_density_bounds(rho_0, rho_h, theta_h, theta_0) { }

void CachedDensityBounds::rebuild_cached_densities(int tree_height){
    m_cached_densities.clear();
    m_cached_densities.reserve(tree_height);
    for(int i = 1; i <= tree_height; i++){
        m_cached_densities.push_back(m_density_bounds.thresholds(tree_height, i));
    }
}

double CachedDensityBounds::get_upper_threshold_root() const noexcept {
    return m_density_bounds.theta_h;
}

double CachedDensityBounds::get_upper_threshold_leaves() const noexcept {
    return m_density_bounds.theta_0;
}

const DensityBounds& CachedDensityBounds::densities() const noexcept {
    return m_density_bounds;
}


int CachedDensityBounds::get_calibrator_tree_height() const noexcept {
    return m_cached_densities.size();
}

} // namespace pma


