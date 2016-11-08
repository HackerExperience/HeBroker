defmodule HeBroker.Mixfile do

  use Mix.Project

  def project do
    [
      app: :hebroker,
      version: "0.1.0",
      elixir: "~> 1.3",
      build_embedded: Mix.env == :prod,
      start_permanent: Mix.env == :prod,
      elixirc_paths: compile_paths(Mix.env),
      package: package,
      deps: deps]
  end

  def application do
    [applications: [:logger]]
  end

  defp deps do
    [
      {:earmark, "~> 1.0", only: :dev},
      {:ex_doc, "~> 0.14", only: :dev},
      {:credo, "~> 0.5", only: [:dev, :test]},
      {:dialyze, "~> 0.2", only: [:dev, :test]}]
  end

  defp compile_paths(:test),
    do: ["lib", "test/helper"]
  defp compile_paths(_),
    do: ["lib"]

  defp package do
    []
  end
end