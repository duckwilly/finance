"""Utilities for generating flavourful synthetic names."""
from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Dict, List, Sequence

from faker import Faker


@dataclass(frozen=True)
class LocaleConfig:
    """Configuration describing how to generate culturally consistent names."""

    label: str
    locale: str
    weight: int


def _list_from_block(block: str) -> list[str]:
    """Turn a newline separated text block into a list of unique entries."""

    seen: set[str] = set()
    values: list[str] = []
    for raw in block.splitlines():
        item = raw.strip()
        if not item or item.startswith("#"):
            continue
        if item in seen:
            continue
        seen.add(item)
        values.append(item)
    return values


NAME_LOCALES: Sequence[LocaleConfig] = (
    LocaleConfig(label="English (GB)", locale="en_GB", weight=3),
    LocaleConfig(label="English (US)", locale="en_US", weight=2),
    LocaleConfig(label="Dutch (NL)", locale="nl_NL", weight=3),
    LocaleConfig(label="Dutch (BE)", locale="nl_BE", weight=1),
)


_FAKERS: Dict[str, Faker] = {cfg.locale: Faker(cfg.locale) for cfg in NAME_LOCALES}


def _choose_locale(rng: random.Random) -> LocaleConfig:
    weights = [cfg.weight for cfg in NAME_LOCALES]
    return rng.choices(NAME_LOCALES, weights=weights, k=1)[0]


def random_person_name(rng: random.Random | None = None) -> tuple[str, str]:
    """Return a (first, last) tuple using Dutch and English Faker providers."""

    rng = rng or random
    locale = _choose_locale(rng)
    faker = _FAKERS[locale.locale]
    faker.random = rng
    first = faker.first_name()
    last = faker.last_name()
    return first, last


GREEK_MYTH_CORES = _list_from_block(
    """
    Aegis
    Aether
    Acheron
    Aeolus
    Amalthea
    Amphitrite
    Andromeda
    Apollo
    Arachne
    Ares
    Arete
    Argo
    Ariadne
    Artemis
    Astraeus
    Atalanta
    Athena
    Atlas
    Atropos
    Boreas
    Briareus
    Cadmus
    Calypso
    Calliope
    Cassandra
    Castor
    Ceto
    Chaos
    Circe
    Clytemnestra
    Cronus
    Cybele
    Damocles
    Daphne
    Demeter
    Dione
    Dionysus
    Electra
    Enyo
    Eos
    Erebus
    Eryx
    Euterpe
    Gaia
    Galatea
    Harmonia
    Hebe
    Hecate
    Helius
    Helios
    Hemera
    Hera
    Heracles
    Hermes
    Hespera
    Hestia
    Hippolyta
    Hyperion
    Iapetus
    Icarus
    Io
    Iris
    Kallisto
    Kratos
    Leto
    Maia
    Medusa
    Meliae
    Melpomene
    Metis
    Nemesis
    Morpheus
    Nike
    Nyx
    Oceanus
    Ophelia
    Orpheus
    Pallas
    Pegasus
    Persephone
    Perses
    Phoebe
    Phosphor
    Poseidon
    Proteus
    Rhea
    Selene
    Styx
    Tethys
    Thalassa
    Thanatos
    Themis
    Theseus
    Tyche
    Zephyrus
    Zeus
    """
)


TOLKIEN_CORES = _list_from_block(
    """
    Amon
    Anduril
    Angband
    Angmar
    Anor
    Anorion
    Arathorn
    Arda
    Argonath
    Arnor
    Athelas
    Baggins
    Balinor
    Balrog
    Barad-dur
    Beleriand
    Belfalas
    Belthronding
    Beorn
    Bifrost
    Bree
    Brisingr
    Caradhras
    Caras Galadhon
    Celeborn
    Cirdan
    Cirith
    Doriath
    Dorwinion
    Dunedain
    Durin
    Eärendil
    Edoras
    Eldamar
    Eldarion
    Eldenroot
    Elendil
    Elrond
    Elros
    Eomer
    Eowyn
    Eregion
    Erebor
    Eredor
    Eriador
    Eruanna
    Estel
    Fangorn
    Faramir
    Finrod
    Forochel
    Forodwaith
    Galadriel
    Gandalf
    Gil-galad
    Glorfindel
    Gondolin
    Gondor
    Gorgoroth
    Helmhammer
    Helmsdeep
    Hithlum
    Imladris
    Isengard
    Isildur
    Ithildin
    Ithilien
    Khazad
    Laurelin
    Lothlorien
    Mandalore
    Minas Ithil
    Minas Tirith
    Mithrandir
    Mithril
    Moria
    Nandor
    Nenya
    Nerevar
    Numenor
    Olorin
    Orthanc
    Palantir
    Pelennor
    Rivendell
    Rohan
    Shadowfax
    Silmaril
    Silvan
    Silverhand
    Stormhold
    Telperion
    Thorin
    Thorongil
    Valinor
    Vilya
    """
)


NAVAL_SHIP_CORES = _list_from_block(
    """
    Achilles
    Active
    Adventure
    Agamemnon
    Ajax
    Albion
    Alcantara
    Alacrity
    Alarm
    Alceste
    Algiers
    Ambuscade
    Anson
    Ark Royal
    Armada
    Arrow
    Artful
    Audacious
    Aurora
    Avenger
    Barham
    Bellerophon
    Bellona
    Benbow
    Berwick
    Birmingham
    Black Prince
    Blake
    Bonaventure
    Boreas
    Boxer
    Bulwark
    Caledonia
    Calliope
    Cambrian
    Canada
    Canterbury
    Captain
    Carcass
    Caroline
    Centaur
    Centurion
    Champion
    Charybdis
    Chatham
    Colossus
    Comet
    Concorde
    Conqueror
    Courageous
    Crescent
    Daedalus
    Dauntless
    Defender
    Delight
    Devastation
    Devonia
    Diana
    Dido
    Dominator
    Dorsetshire
    Dragon
    Dreadnought
    Edgar
    Edinburgh
    Effingham
    Electra
    Endeavour
    Eolus
    Erebus
    Euryalus
    Excellent
    Exmouth
    Falmouth
    Foresight
    Formidable
    Fortitude
    Furious
    Galatea
    Ganges
    Glasgow
    Glatton
    Glory
    Goliath
    Good Hope
    Grappler
    Greyhound
    Griffin
    Havock
    Hawke
    Hector
    Hermes
    Hood
    Hotspur
    Howe
    Illustrious
    Implacable
    Indefatigable
    Indomitable
    Indus
    Inflexible
    Intrepid
    Invincible
    Iron Duke
    Isis
    Javelin
    Juno
    Kent
    King Alfred
    King George
    Lancaster
    Leander
    Lightning
    Lion
    Malaya
    Manchester
    Mars
    Minotaur
    Monarch
    Montagu
    Naiad
    Neptune
    Nereide
    Nestor
    Nonsuch
    Norfolk
    Northumberland
    Ocean
    Orion
    Palliser
    Pegasus
    Penelope
    Perseus
    Phoenix
    Pickle
    Pioneer
    Porpoise
    Powerful
    Prince of Wales
    Prince William
    Proserpine
    Protector
    Queen Charlotte
    Queen Elizabeth
    Quiberon
    Rainbow
    Ramillies
    Ranger
    Rapid
    Renown
    Repulse
    Resolution
    Revenge
    Rodney
    Royal Oak
    Royal Sovereign
    Royalist
    Russell
    Saint George
    Salamander
    Sceptre
    Scipion
    Seahorse
    Serapis
    Shannon
    Sheffield
    Sirius
    Sovereign
    Speedy
    Spencer
    Superb
    Surprise
    Swiftsure
    Talbot
    Temeraire
    Terror
    Theseus
    Thunderer
    Tiger
    Trafalgar
    Trident
    Triumph
    Triton
    Unicorn
    Vanguard
    Vengeance
    Venerable
    Venturer
    Verdun
    Victorious
    Victory
    Vigilant
    Valiant
    Warrior
    Warspite
    Westminster
    Windsor
    Zealous
    Zenith
    Zephyr
    Zulu
    """
)

COMPANY_CORES: List[str] = (
    GREEK_MYTH_CORES
    + TOLKIEN_CORES
    + NAVAL_SHIP_CORES
)


COMPANY_SUFFIXES = [
    "Analytics",
    "Architects",
    "Associates",
    "Capital",
    "Collective",
    "Consulting",
    "Dynamics",
    "Enterprises",
    "Forge",
    "Guild",
    "Holdings",
    "Industries",
    "Innovation",
    "Labs",
    "Logistics",
    "Partners",
    "Solutions",
    "Studios",
    "Systems",
    "Technologies",
    "Ventures",
    "Works",
]


COMPANY_LEGAL_SUFFIXES = ["BV", "NV", "Ltd", "LLC", "Group"]


def random_company_name(rng: random.Random | None = None) -> str:
    """Construct a flavourful company name with a legal suffix."""

    rng = rng or random
    core = rng.choice(COMPANY_CORES)
    suffix = rng.choice(COMPANY_SUFFIXES)
    legal = rng.choice(COMPANY_LEGAL_SUFFIXES)
    return f"{core} {suffix} {legal}"
