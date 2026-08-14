pipeline {
    agent any

    parameters {
        choice(name: 'ENVIRONMENT', choices: ['dev', 'qa', 'uat', 'prod'], description: 'Target environment')
    }

    environment {
        APP_NAME       = 'url_shortener'
        IMAGE_TAG      = "${APP_NAME}:${env.BUILD_NUMBER}"
        NEXUS_REGISTRY = 'nexus:8082'        // Docker hosted repo port (8081 is the Nexus UI/API port, not the registry)
        NEXUS_CREDS_ID = 'nexus-creds'      // Jenkins credential ID, populated from Vault
        VAULT_ADDR     = 'http://hashicorp-vault:8200'
        VAULT_CREDS_ID = 'vault-token'      // Jenkins credential ID for Vault token
    }

    stages {

        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Build') {
            steps {
                sh 'rebar3 compile'
                sh 'rebar3 release'
            }
        }

        stage('Test') {
            steps {
                sh 'rebar3 eunit'
            }
        }

        stage('Docker Build') {
            steps {
                sh "docker build -t ${IMAGE_TAG} ."
            }
        }

        stage('Push to Nexus') {
            steps {
                withCredentials([usernamePassword(credentialsId: "${NEXUS_CREDS_ID}", usernameVariable: 'NEXUS_USER', passwordVariable: 'NEXUS_PASS')]) {
                    sh """
                        echo \$NEXUS_PASS | docker login ${NEXUS_REGISTRY} -u \$NEXUS_USER --password-stdin
                        docker tag ${IMAGE_TAG} ${NEXUS_REGISTRY}/${IMAGE_TAG}
                        docker push ${NEXUS_REGISTRY}/${IMAGE_TAG}
                    """
                }
            }
        }

        stage('Approval') {
            when {
                expression { params.ENVIRONMENT in ['uat', 'prod'] }
            }
            steps {
                input message: "Deploy build ${env.BUILD_NUMBER} to ${params.ENVIRONMENT}?"
            }
        }

        stage('Deploy via Ansible') {
            steps {
                withVault(configuration: [vaultUrl: "${VAULT_ADDR}", vaultCredentialId: "${VAULT_CREDS_ID}"],
                          vaultSecrets: [[path: "secret/${params.ENVIRONMENT}/deploy",
                                          secretValues: [[envVar: 'ANSIBLE_SSH_PASS', vaultKey: 'ssh_pass']]]]) {
                    sh """
                        ansible-playbook -i ansible/inventory/${params.ENVIRONMENT}.ini \
                            ansible/deploy.yml \
                            --extra-vars "image_tag=${NEXUS_REGISTRY}/${IMAGE_TAG} env=${params.ENVIRONMENT}"
                    """
                }
            }
        }
    }

    post {
        success {
            echo "Deployed ${IMAGE_TAG} to ${params.ENVIRONMENT} successfully."
        }
        failure {
            echo "Pipeline failed. Check logs above."
        }
    }
}