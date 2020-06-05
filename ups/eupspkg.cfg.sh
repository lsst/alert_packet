build() {
    msg "No build needed; skipping default"
}

install() {
    # Do not auto-detect the build system; use eupspkg's "just copy
    # everything" approach.

    die_if_empty PRODUCT$
    die_if_empty VERSION

    clean_old_install

    # just copy everything, except for the ups directory
    mkdir -p "$PREFIX"
    cp -a ./ "$PREFIX"
    rm -rf "$PREFIX/ups"
    msg "Copied the product into '$PREFIX'"

    cd "$reldir"

    install_ups
}
