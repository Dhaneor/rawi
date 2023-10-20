#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Provides a generator for random celtic names.

Created on Sun Nov 28 13:12:20 2022

@author dhaneor
"""
import random


names = {
    "celtic": {
        "male": [
            "Aidan", "Bran", "Caelan", "Deirdre", "Eamon", "Faelan", "Lachlan",
            "Maelis", "Orin", "Seamus", "Aoife", "Borin", "Connla", "Fearghus",
            "Lugh", "Neasa", "Padraig", "Ailis", "Colm", "Daireann", "Fiachra",
            "Lir", "Niall", "Tadhg", "Aislinn", "Bevin", "Cian", "Eibhlin",
            "Finian", "Gwendolyn", "Maeve", "Niamh", "Maelis", "Orin", "Sadb",
            "Roisin", "Laoise", "Mairead",
        ],
        "female": [
            "Ailbhe", "Sinead", "Breena", "Dymphna", "Grania", "Brigid", "Grianne",
            "Brighid", "Caoimhe", "Deirdre", "Eibhlin", "Finnguala", "Grainne",
            "Honora", "Iseult", "Liadan", "Maeliosa", "Niamh", "Orla", "Riona",
            "Saoirse", "Aislinn", "Bronagh", "Caitriona", "Dymphna", "Eilidh",
            "Fionnuala", "Gormlaith", "Isibeal", "Laoise", "Mairin", "Nuala",
            "Isolt", "Maeve", "Neasa", "Oonagh", "Sadbh", "Siobhan", "Aisling",
            "Cara", "Oona", "Roisin", "Sile", "Aine", "Caitlin", "Eilis",
            "Fionnghuala", "Gwendolyn", "Eithne", "Fionnghuala", "Gwyneth", "Lir",
            "Maura", "Nessa", "Rhiannon", "Sorcha",
        ]
    },
    "elven": {
        "male": [
            "Elrond", "Legolas", "Celeborn", "Thranduil", "Haldir", "Rúmil",
            "Orophin", "Glorfindel", "Erestor", "Gildor", "Beleg", "Caranthir",
            "Celebrimbor", "Círdan", "Elmo", "Eöl", "Fëanor", "Finarfin",
            "Fingolfin", "Thingol"
        ],
        "female": [
            "Galadriel", "Arwen", "Tauriel", "Nimrodel", "Nienna", "Vána", "Nessa",
            "Yavanna", "Estë", "Idril", "Finduilas", "Lúthien", "Nimloth", "Míriel",
            "Melian", "Nellas", "Nienor", "Niënor", "Saelind", "Uinen"
        ]
    }
}

generations = ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X']


def random_celtic_name(gender="male"):
    """
    Returns a random celtic name.

    Parameters
    ----------
    gender : str, optional
        The gender of the celtic name. The default is "male".

    Returns
    -------
    str
        A random celtic name.
    """
    first = random.choice(names["celtic"][gender])
    second = random.choice(names["celtic"][gender])
    generation = random.choice(generations)

    return f"{first} {second} {generation}."


def random_elven_name(gender="male"):
    """
    Returns a random elven name.

    Parameters
    ----------
    gender : str, optional
        The gender of the elven name. The default is "male".

    Returns
    -------
    str
        A random elven name.
    """
    first = random.choice(names["elven"][gender])
    second = random.choice(names["elven"][gender])
    generation = random.choice(generations)

    return f"{first} {second} {generation}."


if __name__ == "__main__":
    print(random_elven_name())
