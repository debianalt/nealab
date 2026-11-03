"""
Names for the sociodemographic k-means types (3-nov-2026).

aggregate_radio_to_h3.auto_label named each cluster after the variable that deviates
most ("densidad_hab_km2 bajo, nbi alto"), and those column names reached the map's
chip. These names come from the cluster's profile against its province, with one rule
for every province; on the 4 NEA provinces it reproduces the names chosen with the
author:

  ≥ 1,000 inhabitants/km²                         → Urbano denso
  NBI ≥ 1.4 × the province's                       → NBI y hacinamiento altos
                                                    (no density adjective: 44–114 hab/km² across provinces)
  homeowners ≤ the province's − 15 points          → Rural muy disperso, mayoría no propietaria
  ≥ 100 inhabitants/km²                            → Periurbano, NBI bajo
  otherwise                                        → Disperso, NBI bajo

Profiles are means over the cluster's hexagons; province values are means over every
hexagon with census data.
"""

URBAN = 'Urbano denso'
DEPRIVED = 'NBI y hacinamiento altos'
RENTERS = 'Rural muy disperso, mayoría no propietaria'
PERIURBAN = 'Periurbano, NBI bajo'
DISPERSED = 'Disperso, NBI bajo'
NO_CENSUS = 'Sin cobertura censal'


def name_socio_type(density: float, nbi: float, owners: float,
                    prov_nbi: float, prov_owners: float) -> str:
    if density >= 1000:
        return URBAN
    if nbi >= 1.4 * prov_nbi:
        return DEPRIVED
    if owners <= prov_owners - 15:
        return RENTERS
    if density >= 100:
        return PERIURBAN
    return DISPERSED
