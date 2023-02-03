# Steven's Star Schema Project Implementation
![GitHub last commit](https://img.shields.io/github/last-commit/steviedas/steven-repo)
![GitHub commit activity](https://img.shields.io/github/commit-activity/w/steviedas/steven-repo)

This project is based around a bike sharing program, that allows riders to purchase a pass at a kiosk or use a mobile application to unlock a bike at stations around the city and use the bike for a specified amount of time. The bikes can be returned to the same station or another station.

We created a star schema that ingests data into a Bronze layer. The incoming data data in the Bronze layer is then manipulated by enfocing a given schema and saved into the Silver layer. The data in the Silver layer is then transformed into the fact and dimension tables as laid out in the physical database design.

Star schema project files are located within this repository.

## Star schema design
The various stages of the design of the star schema is shown in the various stages below.

   <details>
   <summary>Conceptual database design</summary>

   > The recommended way to install zoxide is via the install script:
   >
   ><p align="center">
   ><img src="https://raw.githubusercontent.com/andresz1/size-limit-action/master/assets/pr.png"
   >  alt="Size Limit comment in pull request about bundle size changes"
   >  width="686" height="289">
   ></p>
   >
   > ```sh
   > curl -sS https://raw.githubusercontent.com/ajeetdsouza/zoxide/main/install.sh | bash
   > ```
   >
   > Or, you can use a package manager:
   >
   > | Distribution        | Repository              | Instructions                                                                                          |
   > | ------------------- | ----------------------- | ----------------------------------------------------------------------------------------------------- |
   > | ***Any***           | **[crates.io]**         | `cargo install zoxide --locked`                                                                       |
   > | *Any*               | [asdf]                  | `asdf plugin add zoxide https://github.com/nyrst/asdf-zoxide.git` <br /> `asdf install zoxide latest` |
   > | *Any*               | [conda-forge]           | `conda install -c conda-forge zoxide`                                                                 |
   > | *Any*               | [Linuxbrew]             | `brew install zoxide`                                                                                 |
   > | Alpine Linux 3.13+  | [Alpine Linux Packages] | `apk add zoxide`                                                                                      |
   > | Arch Linux          | [Arch Linux Community]  | `pacman -S zoxide`                                                                                    |
   > | CentOS 7+           | [Copr]                  | `dnf copr enable atim/zoxide` <br /> `dnf install zoxide`                                             |
   > | Debian 11+[^1]      | [Debian Packages]       | `apt install zoxide`                                                                                  |
   > | Devuan 4.0+[^1]     | [Devuan Packages]       | `apt install zoxide`                                                                                  |
   > | Fedora 32+          | [Fedora Packages]       | `dnf install zoxide`                                                                                  |
   > | Gentoo              | [GURU Overlay]          | `eselect repository enable guru` <br /> `emerge --sync guru` <br /> `emerge app-shells/zoxide`        |
   > | Manjaro             |                         | `pacman -S zoxide`                                                                                    |
   > | NixOS 21.05+        | [nixpkgs]               | `nix-env -iA nixpkgs.zoxide`                                                                          |
   > | openSUSE Tumbleweed | [openSUSE Factory]      | `zypper install zoxide`                                                                               |
   > | Parrot OS[^1]       |                         | `apt install zoxide`                                                                                  |
   > | Raspbian 11+[^1]    | [Raspbian Packages]     | `apt install zoxide`                                                                                  |
   > | Slackware 15.0+     | [SlackBuilds]           | [Instructions][slackbuilds-howto]                                                                     |
   > | Ubuntu 21.04+[^1]   | [Ubuntu Packages]       | `apt install zoxide`                                                                                  |
   > | Void Linux          | [Void Linux Packages]   | `xbps-install -S zoxide`                                                                              |

   </details>

  
1. **Conceptual database design**
2. **Logical database design**
3. **Pysical database design**

## Raw data
Within the repository, there is a folder called 'zips', contianing the relevant .zip files within. These zips contain the raw data in csv files.

Zip Files     | Contents
------------- | -------------
payments.zip  | payments.csv
riders.zip    | riders.csv
stations.zip  | stations.csv
trips.zip     | trips.csv

## Creating the schema
Within the repository, the schemas for the each of the tables in each of the layers is written out in the file "SchemaCreation.py".
An example of one of these schema is written below:

