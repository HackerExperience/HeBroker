defmodule HeBroker.Mixfile do

  use Mix.Project

  def project do
    [
      app: :hebroker,
      version: "0.1.0",
      elixir: "~> 1.3",
      name: "HEBroker",
      description: "A simple broker for RPC and event handling",
      package: package(),
      elixirc_paths: compile_paths(Mix.env),
      deps: deps()
    ]
  end

  def application do
    [applications: [:logger]]
  end

  defp deps do
    [
      {:earmark, "~> 1.0", only: :dev},
      {:ex_doc, "~> 0.14", only: :dev},
      {:credo, "~> 0.5", only: :dev},
      {:dialyxir, "~> 0.4", only: :dev, runtime: false}
    ]
  end

  defp compile_paths(:test),
    do: ["lib", "test/helper"]
  defp compile_paths(_),
    do: ["lib"]

  defp package do
    [
      licenses: ["BSD 3-Clause"],
      files: ~w/mix.exs lib README.md LICENSE CHANGELOG.md/,
      maintainers: ["Charlotte Lorelei Oliveira"],
      links: %{
        "Phabricator" => "https://dev.hackerexperience.com/diffusion/BROKER/",
        "GitHub" => "https://github.com/HackerExperience/HeBroker"
      }
    ]
  end
end