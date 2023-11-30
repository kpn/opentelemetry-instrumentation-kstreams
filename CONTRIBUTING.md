# Contributing

Thanks for deciding to contribute.

## Steps

1. Clone (or fork) this repository
2. Create a new branch with `git switch -c {branch_name}`
2. Make your changes
3. Run
```sh
./scripts/format
./scripts/lint
./scripts/test
```
4. Create a commit
5. Push your code
6. Create a Pull Request

## Adding dependencies

```sh
poetry add {package_name}
```

## Adding dev dependencies

We use the group `dev` for developer deps.

```sh
poetry add --group dev {package_name}
```
