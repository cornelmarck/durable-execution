"""Shared container image macro: multi-arch manifest, local load and registry push."""

load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("@rules_img//img:image.bzl", "image_index", "image_manifest")
load("@rules_img//img:load.bzl", "image_load")
load("@rules_img//img:push.bzl", "image_push")

REGISTRY = "us-east1-docker.pkg.dev"

REPOSITORY_PREFIX = "homelab-464022/homelab-repo"

PLATFORMS = [
    Label("@rules_go//go/toolchain:linux_amd64"),
    Label("@rules_go//go/toolchain:linux_arm64"),
]

# Short git SHA; "dev" if it's unavailable (e.g. outside a git checkout).
TAG = "{{if .STABLE_GIT_SHA}}{{.STABLE_GIT_SHA}}{{else}}dev{{end}}"

def _container_image_impl(name, visibility, base, layers, entrypoint, exposed_ports, repository):
    config_fragment = None
    if exposed_ports:
        write_file(
            name = name + "_config",
            out = name + "_config.json",
            content = [json.encode({"config": {"ExposedPorts": {p: {} for p in exposed_ports}}})],
        )
        config_fragment = ":" + name + "_config"

    image_manifest(
        name = name + "_manifest",
        base = base,
        config_fragment = config_fragment,
        entrypoint = entrypoint,
        layers = layers,
    )

    # Builds the manifest once per platform.
    image_index(
        name = name,
        manifests = [":" + name + "_manifest"],
        platforms = PLATFORMS,
        visibility = visibility,
    )

    # Docker loads one platform at a time; defaults to the host's.
    # Pick another with: bazel run :<name>_load -- --platform linux/amd64
    image_load(
        name = name + "_load",
        image = ":" + name,
        tag = repository + ":dev",
        visibility = visibility,
    )

    image_push(
        name = name + "_push",
        image = ":" + name,
        registry = REGISTRY,
        repository = REPOSITORY_PREFIX + "/" + repository,
        tag = TAG,
        # Always resolve TAG, without needing --stamp. Only the push config is
        # stamped, so image layers stay cacheable.
        stamp = "force",
        visibility = visibility,
    )

container_image = macro(
    doc = """Multi-arch image with `<name>_load` and `<name>_push` targets.

    `name` is the image index; `<name>_manifest` is the per-platform manifest.
    """,
    implementation = _container_image_impl,
    attrs = {
        "base": attr.label(mandatory = True, configurable = False, doc = "Base image, e.g. from `pull()`."),
        "layers": attr.label_list(mandatory = True, doc = "Layers on top of the base, usually `image_layer` targets."),
        "entrypoint": attr.string_list(configurable = False),
        "exposed_ports": attr.string_list(configurable = False, doc = "E.g. `[\"8080/tcp\"]`."),
        "repository": attr.string(mandatory = True, configurable = False, doc = "Repository name under REPOSITORY_PREFIX."),
    },
)
